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
package org.apache.qpid.jms.jndi;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.naming.Binding;
import javax.naming.CompositeName;
import javax.naming.Context;
import javax.naming.LinkRef;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameNotFoundException;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.NotContextException;
import javax.naming.OperationNotSupportedException;
import javax.naming.Reference;
import javax.naming.spi.NamingManager;

/**
 * A read-only Context
 * <p>
 * This version assumes it and all its sub-context are read-only and any attempt
 * to modify (e.g. through bind) will result in an
 * OperationNotSupportedException. Each Context in the tree builds a cache of
 * the entries in all sub-contexts to optimize the performance of lookup.
 * <p>
 * This implementation is intended to optimize the performance of lookup(String)
 * to about the level of a HashMap get. It has been observed that the scheme
 * resolution phase performed by the JVM takes considerably longer, so for
 * optimum performance lookups should be coded like:
 * <code>
 * Context componentContext = (Context)new InitialContext().lookup("java:comp");
 * String envEntry = (String) componentContext.lookup("env/myEntry");
 * String envEntry2 = (String) componentContext.lookup("env/myEntry2");
 * </code>
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ReadOnlyContext implements Context, Serializable {

    public static final String SEPARATOR = "/";
    protected static final NameParser NAME_PARSER = new NameParserImpl();
    private static final long serialVersionUID = 8439856618571886566L;

    protected final Hashtable<String, Object> environment; // environment for this context
    protected final Map<String, Object> bindings; // bindings at my level
    protected final Map<String, Object> treeBindings; // all bindings under me

    private String nameInNamespace = "";

    public ReadOnlyContext() {
        environment = new Hashtable<String, Object>();
        bindings = new HashMap<String, Object>();
        treeBindings = new HashMap<String, Object>();
    }

    public ReadOnlyContext(Hashtable env) {
        if (env == null) {
            this.environment = new Hashtable<String, Object>();
        } else {
            this.environment = new Hashtable<String, Object>(env);
        }
        this.bindings = Collections.EMPTY_MAP;
        this.treeBindings = Collections.EMPTY_MAP;
    }

    public ReadOnlyContext(Hashtable environment, Map<String, Object> bindings) {
        if (environment == null) {
            this.environment = new Hashtable<String, Object>();
        } else {
            this.environment = new Hashtable<String, Object>(environment);
        }
        this.bindings = new HashMap<String, Object>();
        treeBindings = new HashMap<String, Object>();
        if (bindings != null) {
            for (Map.Entry<String, Object> binding : bindings.entrySet()) {
                try {
                    internalBind(binding.getKey(), binding.getValue());
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ReadOnlyContext(Hashtable environment, Map<String, Object> bindings, String nameInNamespace) {
        this(environment, bindings);
        this.nameInNamespace = nameInNamespace;
    }

    protected ReadOnlyContext(ReadOnlyContext clone, Hashtable env) {
        this.bindings = clone.bindings;
        this.treeBindings = clone.treeBindings;
        this.environment = new Hashtable<String, Object>(env);
    }

    protected ReadOnlyContext(ReadOnlyContext clone, Hashtable<String, Object> env, String nameInNamespace) {
        this(clone, env);
        this.nameInNamespace = nameInNamespace;
    }

    /**
     * internalBind is intended for use only during setup or possibly by
     * suitably synchronized super-classes. It binds every possible lookup into a
     * map in each context. To do this, each context strips off one name segment
     * and if necessary creates a new context for it. Then it asks that context
     * to bind the remaining name. It returns a map containing all the bindings
     * from the next context, plus the context it just created (if it in fact
     * created it). (the names are suitably extended by the segment originally
     * lopped off).
     *
     * @param name
     *      the name of the context to bind.
     * @param value
     *      the value to be bound.
     *
     * @return a map populated from the previous and current context.
     *
     * @throws javax.naming.NamingException if an error occurs during the bind.
     */
    protected Map<String, Object> internalBind(String name, Object value) throws NamingException {
        assert name != null && name.length() > 0;

        Map<String, Object> newBindings = new HashMap<String, Object>();
        int pos = name.indexOf('/');
        if (pos == -1) {
            if (treeBindings.put(name, value) != null) {
                throw new NamingException("Something already bound at " + name);
            }
            bindings.put(name, value);
            newBindings.put(name, value);
        } else {
            String segment = name.substring(0, pos);
            assert segment != null;
            assert !segment.equals("");
            Object o = treeBindings.get(segment);
            if (o == null) {
                o = newContext();
                treeBindings.put(segment, o);
                bindings.put(segment, o);
                newBindings.put(segment, o);
            } else if (!(o instanceof ReadOnlyContext)) {
                throw new NamingException("Something already bound where a subcontext should go");
            }
            ReadOnlyContext readOnlyContext = (ReadOnlyContext) o;
            String remainder = name.substring(pos + 1);
            Map<String, Object> subBindings = readOnlyContext.internalBind(remainder, value);
            for (Iterator<Entry<String, Object>> iterator = subBindings.entrySet().iterator(); iterator.hasNext();) {
                Entry<String, Object> entry = iterator.next();
                String subName = segment + "/" + entry.getKey();
                Object bound = entry.getValue();
                treeBindings.put(subName, bound);
                newBindings.put(subName, bound);
            }
        }
        return newBindings;
    }

    protected ReadOnlyContext newContext() {
        return new ReadOnlyContext();
    }

    @Override
    public Object addToEnvironment(String propName, Object propVal) throws NamingException {
        return environment.put(propName, propVal);
    }

    @Override
    public Hashtable<String, Object> getEnvironment() throws NamingException {
        return (Hashtable<String, Object>) environment.clone();
    }

    @Override
    public Object removeFromEnvironment(String propName) throws NamingException {
        return environment.remove(propName);
    }

    @Override
    public Object lookup(String name) throws NamingException {
        if (name.length() == 0) {
            return this;
        }
        Object result = treeBindings.get(name);
        if (result == null) {
            result = bindings.get(name);
        }
        if (result == null) {
            int pos = name.indexOf(':');
            if (pos > 0) {
                String scheme = name.substring(0, pos);
                Context ctx = NamingManager.getURLContext(scheme, environment);
                if (ctx == null) {
                    throw new NamingException("scheme " + scheme + " not recognized");
                }
                return ctx.lookup(name);
            } else {
                // Split out the first name of the path
                // and look for it in the bindings map.
                CompositeName path = new CompositeName(name);

                if (path.size() == 0) {
                    return this;
                } else {
                    String first = path.get(0);
                    Object obj = bindings.get(first);
                    if (obj == null) {
                        throw new NameNotFoundException(name);
                    } else if (obj instanceof Context && path.size() > 1) {
                        Context subContext = (Context) obj;
                        obj = subContext.lookup(path.getSuffix(1));
                    }
                    return obj;
                }
            }
        }
        if (result instanceof LinkRef) {
            LinkRef ref = (LinkRef) result;
            result = lookup(ref.getLinkName());
        }
        if (result instanceof Reference) {
            try {
                result = NamingManager.getObjectInstance(result, null, null, this.environment);
            } catch (NamingException e) {
                throw e;
            } catch (Exception e) {
                throw (NamingException) new NamingException("could not look up : " + name).initCause(e);
            }
        }
        if (result instanceof ReadOnlyContext) {
            String prefix = getNameInNamespace();
            if (prefix.length() > 0) {
                prefix = prefix + SEPARATOR;
            }
            result = new ReadOnlyContext((ReadOnlyContext) result, environment, prefix + name);
        }
        return result;
    }

    @Override
    public Object lookup(Name name) throws NamingException {
        return lookup(name.toString());
    }

    @Override
    public Object lookupLink(String name) throws NamingException {
        return lookup(name);
    }

    @Override
    public Name composeName(Name name, Name prefix) throws NamingException {
        Name result = (Name) prefix.clone();
        result.addAll(name);
        return result;
    }

    @Override
    public String composeName(String name, String prefix) throws NamingException {
        CompositeName result = new CompositeName(prefix);
        result.addAll(new CompositeName(name));
        return result.toString();
    }

    @Override
    public NamingEnumeration list(String name) throws NamingException {
        Object o = lookup(name);
        if (o == this) {
            return new ListEnumeration();
        } else if (o instanceof Context) {
            return ((Context) o).list("");
        } else {
            throw new NotContextException();
        }
    }

    @Override
    public NamingEnumeration listBindings(String name) throws NamingException {
        Object o = lookup(name);
        if (o == this) {
            return new ListBindingEnumeration();
        } else if (o instanceof Context) {
            return ((Context) o).listBindings("");
        } else {
            throw new NotContextException();
        }
    }

    @Override
    public Object lookupLink(Name name) throws NamingException {
        return lookupLink(name.toString());
    }

    @Override
    public NamingEnumeration list(Name name) throws NamingException {
        return list(name.toString());
    }

    @Override
    public NamingEnumeration listBindings(Name name) throws NamingException {
        return listBindings(name.toString());
    }

    @Override
    public void bind(Name name, Object obj) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void bind(String name, Object obj) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void close() throws NamingException {
        // ignore
    }

    @Override
    public Context createSubcontext(Name name) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public Context createSubcontext(String name) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void destroySubcontext(Name name) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void destroySubcontext(String name) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public String getNameInNamespace() throws NamingException {
        return nameInNamespace;
    }

    @Override
    public NameParser getNameParser(Name name) throws NamingException {
        return NAME_PARSER;
    }

    @Override
    public NameParser getNameParser(String name) throws NamingException {
        return NAME_PARSER;
    }

    @Override
    public void rebind(Name name, Object obj) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void rebind(String name, Object obj) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void rename(Name oldName, Name newName) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void rename(String oldName, String newName) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void unbind(Name name) throws NamingException {
        throw new OperationNotSupportedException();
    }

    @Override
    public void unbind(String name) throws NamingException {
        throw new OperationNotSupportedException();
    }

    private abstract class LocalNamingEnumeration implements NamingEnumeration {
        private final Iterator<Entry<String, Object>> i = bindings.entrySet().iterator();

        @Override
        public boolean hasMore() throws NamingException {
            return i.hasNext();
        }

        @Override
        public boolean hasMoreElements() {
            return i.hasNext();
        }

        protected Map.Entry<String, Object> getNext() {
            return i.next();
        }

        @Override
        public void close() throws NamingException {
        }
    }

    private class ListEnumeration extends LocalNamingEnumeration {
        ListEnumeration() {
        }

        @Override
        public Object next() throws NamingException {
            return nextElement();
        }

        @Override
        public Object nextElement() {
            Map.Entry<String, Object> entry = getNext();
            return new NameClassPair(entry.getKey(), entry.getValue().getClass().getName());
        }
    }

    private class ListBindingEnumeration extends LocalNamingEnumeration {
        ListBindingEnumeration() {
        }

        @Override
        public Object next() throws NamingException {
            return nextElement();
        }

        @Override
        public Object nextElement() {
            Map.Entry<String, Object> entry = getNext();
            return new Binding(entry.getKey(), entry.getValue());
        }
    }
}
