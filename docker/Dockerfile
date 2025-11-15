FROM docker.io/library/ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y git gcc make cmake librdmacm-dev rdma-core libibverbs-dev libncurses5-dev libmount-dev libevent-dev libssl-dev python3-pybind11 python3-dev python3-pip libhiredis-dev liburing-dev

ADD . /root/priskv

WORKDIR /root/priskv
RUN mkdir /workspace
RUN make all
RUN cp server/priskv-server /workspace/
RUN cp client/priskv-benchmark /workspace/
RUN cp client/priskv-client /workspace/
RUN cp cluster/client/priskv-cluster-benchmark /workspace/
RUN rm -rf /root/priskv

WORKDIR /workspace
ENTRYPOINT ["/workspace/priskv-server"]
