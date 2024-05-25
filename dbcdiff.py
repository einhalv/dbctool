import dbctool
import sys

fname1 = sys.argv[1]
fname2 = sys.argv[2]

p1 = dbctool.Parser()
p2 = dbctool.Parser()

with open(fname1, "r") as fp:
    dbcstr1 = fp.read()
with open(fname2, "r") as fp:
    dbcstr2 = fp.read()

b1 = dbctool.Bus(p1.parse(dbcstr1))
b2 = dbctool.Bus(p2.parse(dbcstr2))

diffstr = b1.diff(b2)
if diffstr:
    print(diffstr)
