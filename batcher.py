import time
import sys
import random
import subprocess
random.seed(time.time())

def generate_set_batch(set_batch_filename, num_writes, get_ratio, num_keys):
	#generate random key-values to write
	kv_pairs = []
	for i in range(num_writes):
		kv_pairs.append([random.randint(0, num_keys), random.randint(0, 500)])

	with open(set_batch_filename, 'w') as file:
		for index, kv_pair in enumerate(kv_pairs):
			file.write("SET {} {}\n".format(kv_pair[0], kv_pair[1]))
			if int(index*get_ratio) != int((index-1)*get_ratio):
				file.write("GET {}\n".format(kv_pair[0]))

def generate_get_batch(get_batch_filename, num_keys):
	with open(get_batch_filename, 'w') as file:
		for key in range(num_keys):
			file.write("GET {}\n".format(key))

def generate_batch_files(set_batch1, set_batch2, get_batch, balancing_batch):
	# concurrent set/get testing
	key_set_size = 50
	generate_set_batch(set_batch1, 1000, 0.05, key_set_size)
	generate_set_batch(set_batch2, 1000, 0.05, key_set_size)

	generate_get_batch(get_batch, key_set_size)

	# writing a lot of keys for testing load balancing
	generate_set_batch(balancing_batch, 1000, 0.0, 2000)

def get_possible_values(kv_all, set_batch):
	kv_tmp = {}
	with open(set_batch) as file:
		for line in file:
			items = line.split()
			if(items[0]=='SET'):
				kv_tmp[items[1]] = items[2]
	for key in kv_tmp:
		if kv_all.has_key(key) is False:
			kv_all[key] = {}
		kv_all[key][kv_tmp[key]] = True

	# with open('result.txt', 'w') as file:
	# 	for i in range(50):
	# 		file.write("Found: {}\n".format(kv_all[str(i)].keys()[0]))


def check_result(kv_all, get_batch, result):
	get_keys = []
	with open(get_batch) as file:
		for line in file:
			items = line.split()
			assert items[0] == 'GET', "there should be only GET in {}".format(get_batch)
			get_keys.append(items[1])
	i = 0
	with open(result) as file:
		for line in file:
			items = line.split()
			if kv_all.has_key(get_keys[i]):
				assert items[0] == 'Found:', "key {} should exist, but result shows otherwise in line {}".format(get_keys[i], i)
				all_values = kv_all[get_keys[i]]
				assert all_values.has_key(items[1]), "your value {} for key {} doesn't exist in value-set: {}".format(items[1], get_keys[i], all_values)
			else:
				assert items[0] == 'Not', "key {} should not exist, but result shows otherwise in line {}".format(get_keys[i], i)
			i+=1

def check_concurrent_result(set_batch1, set_batch2, get_batch, result):
	kv_all = {}
	get_possible_values(kv_all, set_batch1)
	get_possible_values(kv_all, set_batch2)
	check_result(kv_all, get_batch, result)
	print "perfect!"

def clear():
	subprocess.call("rm *.txt", shell=True)

def main():
	set_batch1 = "cocurrent_set_1.txt"
	set_batch2 = "cocurrent_set_2.txt"
	get_batch = "cocurrent_get.txt"
	balance_batch = "balancing.txt"
	result = "result.txt"

	if len(sys.argv) == 1:
		print "use it as: python batcher.py check <set_batch1> <set_batch2> <get_batch> <result>"
		print "or: python batcher.py clear"
		print "or: python batcher.py generate <set_batch1> <set_batch2> <get_batch> <balancing_batch>"
		print "Example: python batcher.py check cocurrent_set_1.txt cocurrent_set_2.txt cocurrent_get.txt result.txt"
		exit(0)

	if sys.argv[1] == 'check':
		check_concurrent_result(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
		#check_concurrent_result(set_batch1, set_batch2, get_batch, result)
	elif sys.argv[1] == 'clear':
		clear()
	elif sys.argv[1] == 'generate':
		generate_batch_files(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
		#generate_batch_files(set_batch1, set_batch2, get_batch, balance_batch)

	
	
	

if __name__ == '__main__':
	main()