cd docker/analyzer
make
cd ../..
cd docker/dashboard
make
cd ../
docker-compose up -d
# remove -d if needed