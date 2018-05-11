# The various SSL stores and certificates were created with the following commands:

# Clean up existing files
# -----------------------
rm -f *.crt *.csr *.keystore *.truststore

# Create a key and self-signed certificate for the CA, to sign certificate requests and use for trust:
# ----------------------------------------------------------------------------------------------------
keytool -storetype jks -keystore ca-jks.keystore -storepass password -keypass password -alias ca -genkey -keyalg "RSA" -keysize 2048 -dname "O=My Trusted Inc.,CN=my-ca.org" -validity 9999 -ext bc:c=ca:true
keytool -storetype jks -keystore ca-jks.keystore -storepass password -alias ca -exportcert -rfc > ca.crt

# Create a key pair for the broker, and sign it with the CA:
# ----------------------------------------------------------
keytool -storetype jks -keystore broker-jks.keystore -storepass password -keypass password -alias broker -genkey -keyalg "RSA" -keysize 2048 -dname "O=Server,CN=localhost" -validity 9999 -ext bc=ca:false -ext eku=sA

keytool -storetype jks -keystore broker-jks.keystore -storepass password -alias broker -certreq -file broker.csr
keytool -storetype jks -keystore ca-jks.keystore -storepass password -alias ca -gencert -rfc -infile broker.csr -outfile broker.crt -validity 9999 -ext bc=ca:false -ext eku=sA

keytool -storetype jks -keystore broker-jks.keystore -storepass password -keypass password -importcert -alias ca -file ca.crt -noprompt
keytool -storetype jks -keystore broker-jks.keystore -storepass password -keypass password -importcert -alias broker -file broker.crt

# Create trust stores for the client, import the CA cert:
# -------------------------------------------------------
keytool -storetype jks -keystore client-jks.truststore -storepass password -keypass password -importcert -alias ca -file ca.crt -noprompt
