nohup ./mini-dfs -app dn -dn-addr 127.0.0.1:8081 -nn-addr 127.0.0.1:8080 > ./log/dn1.log 2>&1 &
nohup ./mini-dfs -app dn -dn-addr 127.0.0.1:8082 -nn-addr 127.0.0.1:8080 > ./log/dn2.log 2>&1 &
nohup ./mini-dfs -app dn -dn-addr 127.0.0.1:8083 -nn-addr 127.0.0.1:8080 > ./log/dn3.log 2>&1 &
nohup ./mini-dfs -app dn -dn-addr 127.0.0.1:8084 -nn-addr 127.0.0.1:8080 > ./log/dn4.log 2>&1 &