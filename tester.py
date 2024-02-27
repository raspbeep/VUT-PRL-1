import os
import random
import subprocess
import sys

SCRIPT_PATH = "./generate_numbers.sh"

class InvalidSort(Exception):
    pass

def check_output(out):
    
    numbers_list = [x.strip(' ') for x in out.split('\n')[:-1]]
    if numbers_list == ['']:
        print('[ OK ]')
        return
    original = [int(x) for x in numbers_list[0].split(' ')]
    numbers_list.pop(0)
    numbers_list = [int(x) for x in numbers_list]
    original.sort()
    # print(numbers_list)
    print(original)
    if numbers_list != original:
        raise InvalidSort('Not Sorted')
    else:
        print('[ OK ]')

def run(count_numbers):
    try:
        command = [SCRIPT_PATH, str(count_numbers)]
        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"########################################")
        check_output(result.stdout.decode())
    except InvalidSort as e:
        print(f"Array was not returned sorted.")
        raise Exception
    except subprocess.CalledProcessError as e:
        print(f"Error executing shell script: {e.stderr.decode()}", file=sys.stderr)
        raise Exception
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        raise Exception

if __name__ == "__main__":
    n_tests = 100
    try:
        for i in range(n_tests):
            run(count_numbers=random.randint(0, 1000))
    except Exception as e:
        exit(-1)
