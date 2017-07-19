for line in open("sample_sheet.txt"):

    s = line.split(" ")

    s[1] = s[1].replace(s[2], "_".join([s[2], s[3], s[4]]))

    print(" ".join([s[1], s[0], s[-1].strip()]))
