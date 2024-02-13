/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.jms.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.qpid.jms.util.RefPool.Ref;

final class SharedRefPool<T> implements Supplier<Ref<T>>{

   private static final class AtomicCloseableRef<T> implements Ref<T> {

      private final Ref<T> sharedRef;
      private final AtomicBoolean closed;

      public AtomicCloseableRef(final Ref<T> sharedRef) {
         this.sharedRef = sharedRef;
         this.closed = new AtomicBoolean();
      }

      @Override
      public T ref() {
         return sharedRef.ref();
      }

      @Override
      public void close() {
         if (closed.compareAndSet(false, true)) {
            sharedRef.close();
         }
      }
   }

   private static final class SharedRef<T> implements Ref<T> {

      private final T ref;
      private final AtomicInteger refCnt;
      private final Consumer<? super SharedRef<T>> onDispose;

      public SharedRef(final T ref, final Consumer<? super SharedRef<T>> onDispose) {
         this.ref = ref;
         this.refCnt = new AtomicInteger(1);
         this.onDispose = onDispose;
      }

      public boolean retain() {
         while (true) {
            final int currValue = refCnt.get();
            if (currValue == 0) {
               // this has been already disposed!
               return false;
            }
            if (refCnt.compareAndSet(currValue, currValue + 1)) {
               return true;
            }
         }
      }

      @Override
      public T ref() {
         if (refCnt.get() == 0) {
            throw new IllegalStateException("the ref cannot be used anymore");
         }
         return ref;
      }

      @Override
      public void close() {
         while (true) {
            final int currValue = refCnt.get();
            if (currValue == 0) {
               return;
            }
            if (refCnt.compareAndSet(currValue, currValue - 1)) {
               if (currValue == 1) {
                  onDispose.accept(this);
               }
               return;
            }
         }
      }
   }

   private final Supplier<? extends T> create;
   private final Consumer<? super T> onDispose;
   private final AtomicReference<SharedRef<T>> sharedRef = new AtomicReference<>(null);

   public SharedRefPool(final Supplier<? extends T> create, final Consumer<? super T> onDispose) {
      this.create = create;
      this.onDispose = onDispose;
   }

   @Override
   public Ref<T> get() {
      while (true) {
         SharedRef<T> sharedRef = this.sharedRef.get();
         if (sharedRef != null && sharedRef.retain()) {
            return new AtomicCloseableRef(sharedRef);
         }
         synchronized (this) {
            sharedRef = this.sharedRef.get();
            if (sharedRef != null && sharedRef.retain()) {
               return new AtomicCloseableRef(sharedRef);
            }
            sharedRef = new SharedRef<>(create.get(), self -> {
               // help GC in case no racing get would replace
               this.sharedRef.compareAndSet(self, null);
               onDispose.accept(self.ref);
            });
            this.sharedRef.set(sharedRef);
            return new AtomicCloseableRef(sharedRef);
         }
      }
   }
}
