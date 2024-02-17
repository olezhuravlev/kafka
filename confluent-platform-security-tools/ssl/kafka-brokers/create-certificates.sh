#!/usr/bin/env bash
CERTIFICATE="caroot"
CN="localhost"
PASSWORD="123456"

#############################################################################
# keytool and openssl subj parameters:
#Country Name (2 letter code) [AU]:
SUBJ_C="RU"
#State or Province Name (full name) [Some-State]:
SUBJ_ST="Nsk"
#Locality Name (eg, city) []:
SUBJ_L="Nsk"
#Organization Name (eg, company) [Internet Widgits Pty Ltd]:
SUBJ_O="Grossbook Inc."
#Organizational Unit Name (eg, section) []:
SUBJ_OU="Development"
#Common Name (e.g. server Fully Qualified Domain Name or YOUR name) []:
SUBJ_CN=$CN
# Email Address []:
SUBJ_emailAddress="test@grossbook.org"

echo "--------------------------------------------------------------------------------------------"
echo "#1. Create 'kafka.keystore.jks' with self-signed certificate within."
keytool -keystore kafka.keystore.jks -alias $CN -keyalg RSA -genkey -storepass $PASSWORD \
-dname "CN=$SUBJ_CN,ST=$SUBJ_ST,L=$SUBJ_L,O=$SUBJ_O,OU=$SUBJ_OU,C=$SUBJ_C,emailAddress=$SUBJ_emailAddress"

echo "--------------------------------------------------------------------------------------------"
echo "#2. Export certificate from the 'kafka.keystore.jks' to file 'cert-req-file'."
keytool -keystore kafka.keystore.jks -alias $CN -certreq -file cert-req-file -storepass $PASSWORD

echo "--------------------------------------------------------------------------------------------"
echo "#3. Create CA issuing root certificate to file 'ca-cert' and private key to file 'ca-priv-key'."
openssl req -new -x509 -out ca-cert -keyout ca-priv-key -days 999 \
-subj "/CN=$SUBJ_CN/ST=$SUBJ_ST/L=$SUBJ_L/O=$SUBJ_O/OU=$SUBJ_OU/C=$SUBJ_C/emailAddress=$SUBJ_emailAddress" \
-passin pass:$PASSWORD -passout pass:$PASSWORD

echo "--------------------------------------------------------------------------------------------"
echo "#4. Add the root certificate to 'kafka.keystore.jks' and 'kafka.truststore.jks'."
keytool -keystore kafka.keystore.jks -alias $CERTIFICATE -importcert -file ca-cert -storepass $PASSWORD -noprompt
keytool -keystore kafka.truststore.jks -alias $CERTIFICATE -importcert -file ca-cert -storepass $PASSWORD  -noprompt

echo "--------------------------------------------------------------------------------------------"
echo "#5. CA signs certificate, exported from 'kafka.keystore.jks', and stores it to 'cert-signed'"
openssl x509 -req -CA ca-cert -CAkey ca-priv-key -in cert-req-file -out cert-signed -days 999 -CAcreateserial -passin pass:$PASSWORD

echo "--------------------------------------------------------------------------------------------"
echo "#6. Install the signed certificate in the 'kafka.keystore.jks' (original certificate must be presented)."
keytool -keystore kafka.keystore.jks -alias $CN -importcert -file cert-signed -storepass $PASSWORD

echo "--------------------------------------------------------------------------------------------"
#echo "#7. Clean up files"
#rm ca-cert ca-priv-key cert-req-file cert-signed
