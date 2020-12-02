# Qpid JMS

[Qpid JMS](https://qpid.apache.org/components/jms/) is a JMS 2.0 client that uses
the AMQP 1.0 protocol, enabling it to interact with various AMQP 1.0 servers.

Below are some quick pointers you might find useful.

## Building the code

The project requires Maven 3. Some example commands follow.

Clean previous builds output and install all modules to local repository without
running the tests:

    mvn clean install -DskipTests

Install all modules to the local repository after running all the tests:

    mvn clean install

Perform a subset tests on the packaged release artifacts without
installing:

    mvn clean verify -Dtest=TestNamePattern*

Execute the tests and produce code coverage report:

    mvn clean test jacoco:report

## Examples

First build and install all the modules as detailed above (if running against
a source checkout/release, rather than against released binaries) and then
consult the README in the qpid-jms-examples module itself.

## Documentation

Documentation source can be found in the qpid-jms-docs module, with a published
version available on the [website](https://qpid.apache.org/components/jms/).

## Distribution assemblies

After building the modules, src and binary distribution assemblies can be found at:

    apache-qpid-jms/target

