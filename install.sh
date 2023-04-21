#!bin/bash

wget https://r.mariadb.com/downloads/mariadb_repo_setup
echo "06c500296164e49d0cc8c08ec7b0303445f3ddd7c864870d9e9ae6d159544d0a  mariadb_repo_setup"     | sha256sum -c -
chmod +x mariadb_repo_setup
sudo ./mariadb_repo_setup    --mariadb-server-version="mariadb-10.6"
sudo apt install libmariadb3 libmariadb-dev
source bin/activate
python3 -m pip install -r requirements.txt

