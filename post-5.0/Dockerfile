FROM debian:10.1
ARG branch
ARG mongoshell_package
ARG version
RUN apt-get update && \
    apt-get install -y git python3 python3-pip gcc libcurl4-openssl-dev libssl-dev libffi-dev python-dev wget && \
    git clone --branch $branch https://github.com/mongodb/mongo.git && \
    pip3 install --upgrade pip && \
    pip3 install --user -r /mongo/etc/pip/dev-requirements.txt && \
    pip3 install --user dnspython==1.16.0 && \
    rm -rf /var/lib/apt/lists/* /tmp/* /mongodb-shell-linux-x86_64-$mongoshell_package.tgz
COPY entrypoint.sh /entrypoint.sh
COPY test-suites/$version/* /mongo/buildscripts/resmokeconfig/suites/
RUN rm -f /mongo/buildscripts/resmokelib/testing/testcases/jstest.py
COPY jstest.py /mongo/buildscripts/resmokelib/testing/testcases/
COPY mongo /mongoShell/
ENV m=/mongoShell/mongo
ENV command="resmoke.py run"
ADD https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem /usr/local/share/ca-certificates/rds-ca-2019-root.crt
RUN update-ca-certificates && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    ln -sf /usr/bin/pip3 /usr/bin/pip
ENTRYPOINT ["/entrypoint.sh"]
RUN ["chmod", "+x", "/entrypoint.sh"]

# removed --depth 1 as it was causing issues with resolving test tags in the 5.1 branch
