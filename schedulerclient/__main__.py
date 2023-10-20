import os, getopt, sys
from schedulerclient.apirequest.apirequester import ApiRequester
from dotenv import load_dotenv

def print_usage():
    print('python3 -m schedulerclient name [--help] [--deploy=name,cpu,mem,ratio,disk] [--remove=name] [--url=url] [--status]')
    print('If no url is specified, the environment variable SCG_URL and SCG_PORT will be used')

def retrieve_deploy_args(request_as_str : str):
    config = current_value.split(',')
    try:
        name = config[0]
        cpu = int(config[1])
        memory = int(config[2])
        ratio = int(config[3])
        disk = config[4]
    except Exception:
        print_usage()
        sys.exit(-1)
    return name, cpu, memory, ratio, disk

if __name__ == '__main__':

    short_options = 'hd:r:u:s'
    long_options = ['help', 'deploy=', 'remove=', 'url=', 'status']
 
    try:
        arguments, values = getopt.getopt(sys.argv[1:], short_options, long_options)
    except getopt.error as err:
        print (str(err)) # Output error, and return with an error code
        sys.exit(2)

    load_dotenv()
    SCHEDULERGLOBAL_URL = 'http://' + os.getenv('SCG_URL') + ':' + os.getenv('SCG_PORT')

    deployment = False
    removal = False
    status = False
    for current_argument, current_value in arguments:
        if current_argument in ('-h', '--help'):
            print_usage()
            sys.exit(0)
        elif current_argument in ('-d', '--deploy'):
            name, cpu, memory, ratio, disk = retrieve_deploy_args(current_value)
            deployment, removal = (True, False)
        elif current_argument in ('-r', '--remove'):
            name = current_value
            deployment, removal = (False, True)
        elif current_argument in ('u', '--url'):
            SCHEDULERGLOBAL_URL = current_value
        elif current_argument in ('s', '--status'):
            status = True
        else:
            print_usage()
            sys.exit(-1)

    # Treat request
    requester = ApiRequester(url=SCHEDULERGLOBAL_URL)
    if deployment: print(requester.deploy(name=name, cpu=cpu, memory=memory, ratio=ratio, disk=disk))
    if removal: print(requester.remove(name=name))
    if status: print(requester.status())