from bcrypt import checkpw

def verify_password(plain_text: str, hashed_password: str):
    try:
        return checkpw(plain_text.encode("utf-8"), hashed_password.encode("utf-8"))
    except:
        return False