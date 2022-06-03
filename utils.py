def base10_to_base2_str(num):
    if num % 2 != 0:
        return 0
    div = num // 2
    r = num % 2
    num_base2 = str(r)
    while div != 1 and div != 0:
        num_base2 = str(r) + num_base2
        div = div // 2
        r = div % 2
    res = str(div) + num_base2
    if len(res) < 8:
        while len(res) != 8:
            res = '0' + res
    return res