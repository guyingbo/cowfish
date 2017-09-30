

def format_params(params):
    return ', '.join('{}={}'.format(k, v) for k, v in params.items())
