import re

class QueryError(Exception):
    pass


def check_status(data):
    status = data['status']

    if status == 0:
        return
    elif status == 400:
        message = data.get('object', ['400 error'])[0]
    # missing from clause or other access error (ie oauth token problem)
    elif status == 403:
        message = process_403(data)
    # date issue
    # general query issue
    elif status == 500:
        message = process_500(data)
    else:
        message = status

    raise QueryError(message)


def process_403(data):
    error = data.get('error', '')
    msg = data.get('msg', '')

    if error == "Access not allowed for table 'Nothing'":
        return error + ". Possible error in from clause"
    elif not (error or msg):
        return '403 error'
    else:
        return '. '.join(m for m in [msg, error] if m)


def process_500(data):
    try:
        full_error = data.get('object')[1]
    except:
        error = data.get('error', '')
        msg = data.get('msg', '')
        if not (error or msg):
            return '500 error'
        else:
            return '. '.join(m for m in [msg, error] if m)

    exception_pattern = r'Error from server: malote\.(.*?): '
    linq_pattern = r'as an Linq query\. Error:(.*)'
    error_pattern = r'malote\.{exception}: (.*?) \[MConnectionImpl\[address'

    match = re.search(exception_pattern, full_error)
    try:
        exception_type = match.group(1)

        if exception_type == 'code.CodeParseException':
            message = re.search(linq_pattern, full_error).group(1)
        elif exception_type in ('base.StaticException', 'typing.TypingException'):
            error_pattern = error_pattern.format(exception=exception_type)
            message = re.search(error_pattern, full_error).group(1)
        else:
            message = exception_type

        return message.replace('<?>@', 'Row,Column').strip()

    except:
        return 'API V2 error'
