SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f "../.env" ]; then
    source ../.env
elif [ -f ".env" ]; then
    source .env
else
    echo "ERROR: File .env doesn't exist."
    exit 1
fi

PASSWORD=$KAFKA_SSL_PASSWORD

if [ -z "$PASSWORD" ]; then
    echo "ERROR: Variable KAFKA_SSL_PASSWORD is not set in the .env file!"
    exit 1
fi

DAYS_VALID=40

rm -rf kafka-1-creds kafka-2-creds kafka-3-creds client-creds ca.*
mkdir kafka-1-creds kafka-2-creds kafka-3-creds client-creds

echo "Generation of Certificate Authority..."
openssl req -new -x509 -keyout ca.key -out ca.crt -days $DAYS_VALID -nodes -subj "/CN=FlightFlowCA"

echo "Generation of KeyStore and Truststore for Kafka brokers"
for i in 1 2 3
do
    echo "   -> Configuring kafka-$i..."
    keytool -genkeypair -alias kafka-$i -keyalg RSA -keysize 2048 \
        -keystore kafka-$i-creds/kafka.kafka-$i.keystore.pkcs12 \
        -validity $DAYS_VALID -storepass $PASSWORD -keypass $PASSWORD -dname "CN=kafka-$i"

    keytool -certreq -alias kafka-$i -keystore kafka-$i-creds/kafka.kafka-$i.keystore.pkcs12 \
        -file kafka-$i.csr -storepass $PASSWORD

    openssl x509 -req -CA ca.crt -CAkey ca.key -in kafka-$i.csr -out kafka-$i-creds/kafka-$i.crt -days $DAYS_VALID -CAcreateserial

    keytool -import -trustcacerts -alias CARoot -file ca.crt \
        -keystore kafka-$i-creds/kafka.kafka-$i.keystore.pkcs12 -storepass $PASSWORD -noprompt

    keytool -import -alias kafka-$i -file kafka-$i-creds/kafka-$i.crt \
        -keystore kafka-$i-creds/kafka.kafka-$i.keystore.pkcs12 -storepass $PASSWORD -noprompt

    keytool -import -trustcacerts -alias CARoot -file ca.crt \
        -keystore kafka-$i-creds/kafka.kafka-$i.truststore.pkcs12 -storepass $PASSWORD -noprompt

    rm kafka-$i.csr kafka-$i-creds/kafka-$i.crt
done

echo "Generation of Certificates for Python clients..."

openssl genrsa -out client-creds/kafka.client.key 2048
openssl req -new -key client-creds/kafka.client.key -out client-creds/client.csr -subj "/CN=FlightClient"
openssl x509 -req -CA ca.crt -CAkey ca.key -in client-creds/client.csr \
    -out client-creds/kafka.client.certificate.pem -days $DAYS_VALID -CAcreateserial

rm client-creds/client.csr

echo ""
echo "All certificates have been generated successfully."