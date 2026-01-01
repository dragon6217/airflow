#s04_l18
def get_sftp():
    print('sftp 작업을 시작합니다')
    

#s04_l21
def regist(name, sex, *args):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')


#s04_l22
def regist2(name, sex, *args, **kwargs):
    print(f'이름: {name}')
    print(f'성별: {sex}')
    print(f'기타옵션들: {args}')
    print(f'전체 kwargs: {kwargs}')
    email = kwargs['email'] or None
    phone = kwargs['phone'] or None
    if email:
        print(email)
    if phone:
        print(phone)