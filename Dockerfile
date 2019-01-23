FROM debian:9.6
COPY test_suites_369 /test_suites_369
COPY dev-requirements.txt /dev-requirements.txt
COPY components /components
RUN apt-get update && \
    apt-get install -y git python2.7 python-pip gcc libcurl4-openssl-dev libssl-dev wget && \
    git clone --depth 1 --branch v3.6.9-dbaas-testing https://github.com/mongodb/mongo.git && \
    pip install --user -r /dev-requirements.txt && \
    cp /test_suites_369/* /mongo/buildscripts/resmokeconfig/suites && \
    wget https://downloads.mongodb.org/linux/mongodb-shell-linux-x86_64-debian92-3.6.9.tgz && \
    tar xzf mongodb-shell-linux-x86_64-debian92-3.6.9.tgz && \
    rm -rf /var/lib/apt/lists/* /tmp/* /mongodb-shell-linux-x86_64-debian92-3.6.9.tgz
COPY entrypoint.sh /entrypoint.sh
ENV m36='/mongodb-linux-x86_64-debian92-3.6.9/bin/mongo'
ADD https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem /usr/local/share/ca-certificates/rds-combined-ca-bundle.crt
RUN update-ca-certificates
ENTRYPOINT ["/entrypoint.sh"]
