import os, getopt, sys
from dotenv import load_dotenv
from schedulerglobal.schedulerglobal import SchedulerGlobal

SCHEDULERLOCAL_LIST = ''

def print_usage():
    print('python3 -m schedulerglobal name [--help] [--list=url1,url2]')
    print('If no url list is specified, the environment variable SCG_NODE_URL_LIST will be used')

if __name__ == '__main__':

    short_options = 'hl:'
    long_options = ['help', 'list=']
 
    try:
        arguments, values = getopt.getopt(sys.argv[1:], short_options, long_options)
    except getopt.error as err:
        print (str(err)) # Output error, and return with an error code
        sys.exit(2)

    load_dotenv()
    SCG_URL   = os.getenv('SCG_URL')
    SCG_PORT  = int(os.getenv('SCG_PORT'))
    SCG_DELAY = int(os.getenv('SCG_DELAY'))
    SCHEDULERLOCAL_LIST = os.getenv('SCG_NODE_URL_LIST')

    for current_argument, current_value in arguments:
        if current_argument in ("-l", "--list"):
            SCHEDULERLOCAL_LIST = current_value
        elif current_argument in ("-h", "--help"):
            print_usage()
            sys.exit(0)
        else:
            print_usage()
            sys.exit(-1)
    
    global_scheduler = SchedulerGlobal(url_list=SCHEDULERLOCAL_LIST.split(','),\
            api_url=SCG_URL, api_port=SCG_PORT,\
            delay=SCG_DELAY)
    global_scheduler.run()