#!/usr/bin/env bash

CERTIFICATE_1="caroot-1"
CERTIFICATE_2="caroot-2"
CERTIFICATE_3="caroot-3"
CERTIFICATE_CLIENT="caroot-client"
PASSWORD="123456"

cd ./kafka-broker-1
source ../create-certificates.sh

cd .././kafka-broker-2
source ../create-certificates.sh

cd .././kafka-broker-3
source ../create-certificates.sh

cd .././client
source ../create-certificates.sh

cd ../

echo "--------------------------------------------------------------------------------------------"
echo "*** Add 'ca-cert' of each CA (including client's) to 'kafka.truststore.jks' of each other server ***"
keytool -keystore ./kafka-broker-1/kafka.truststore.jks -alias $CERTIFICATE_2 -importcert -file ./kafka-broker-2/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./kafka-broker-1/kafka.truststore.jks -alias $CERTIFICATE_3 -importcert -file ./kafka-broker-3/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./kafka-broker-1/kafka.truststore.jks -alias CERTIFICATE_CLIENT -importcert -file ./client/ca-cert -storepass $PASSWORD  -noprompt

keytool -keystore ./kafka-broker-2/kafka.truststore.jks -alias $CERTIFICATE_1 -importcert -file ./kafka-broker-1/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./kafka-broker-2/kafka.truststore.jks -alias $CERTIFICATE_3 -importcert -file ./kafka-broker-3/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./kafka-broker-2/kafka.truststore.jks -alias CERTIFICATE_CLIENT -importcert -file ./client/ca-cert -storepass $PASSWORD  -noprompt

keytool -keystore ./kafka-broker-3/kafka.truststore.jks -alias $CERTIFICATE_1 -importcert -file ./kafka-broker-1/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./kafka-broker-3/kafka.truststore.jks -alias $CERTIFICATE_2 -importcert -file ./kafka-broker-2/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./kafka-broker-3/kafka.truststore.jks -alias CERTIFICATE_CLIENT -importcert -file ./client/ca-cert -storepass $PASSWORD  -noprompt

echo "--------------------------------------------------------------------------------------------"
echo "*** Add 'ca-cert' of each CA to 'kafka.truststore.jks' of the client ***"
keytool -keystore ./client/kafka.truststore.jks -alias $CERTIFICATE_1 -importcert -file ./kafka-broker-1/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./client/kafka.truststore.jks -alias $CERTIFICATE_2 -importcert -file ./kafka-broker-2/ca-cert -storepass $PASSWORD  -noprompt
keytool -keystore ./client/kafka.truststore.jks -alias $CERTIFICATE_3 -importcert -file ./kafka-broker-3/ca-cert -storepass $PASSWORD  -noprompt
