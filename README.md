Script:

git clone https://github.com/XueweiKent/428mp2demo

cp ./428mp2demo/* .

then in machine 1: “BATCH concurrent_set_1.txt tmp”

and in machine 2: “BATCH concurrent_set_2.txt tmp”

wait for both of them to finish, 

then in machine 1: “BATCH concurrent_get.txt result1.txt”

then in machine 2: “BATCH concurrent_get.txt result2.txt”

wait for both of them to finish, and scp them into same folder

diff result1.txt result2.txt

python batcher.py check concurrent_set_1.txt concurrent_set_2.txt concurrent_get.txt result1.txt
