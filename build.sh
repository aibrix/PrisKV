set +e
make clean
set -e
make all PRISKV_USE_CUDA=1
rm -rf output
mkdir -p output
cp server/priskv-server output/
cp client/priskv-benchmark output/
cp client/priskv-client output/
cp cluster/client/priskv-cluster-benchmark output/
