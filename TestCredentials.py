import botocore.session as botosession 

def print_credentials():
    s = botosession.get_session()
    c = s.get_credentials()
    
    if not c is None:
        print('Access Key', c.access_key)
        print('Secrey Key', c.secret_key)
        print('Token', c.token)
    else:   
        print('No credentials found')


if __name__ == '__main__':    
    print_credentials()