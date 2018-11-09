import pandas

df = pandas.read_csv('event-data-extract-v0.1.csv')

i=0
# print(df)
# print(df.loc[0].iat[0])
for rr in df:

    # print(df.loc[i].iat[0])
    rr=df.loc[i].iat[0]
    i=i+1
    item = rr.split('\t')
    for aa in item:
        print(aa)


#     if i==0:
#         tit=item
#         break
#
#     for aa in item:
#         print(aa)
#         break
#     break
#
# for bb in tit:
#     bb=bb.split('\"')
#     if len(bb)>1:
#         bb=bb[1]
#     else:
#         bb=bb[0]
#
#     bb=bb.replace(".", "_")
#     if bb=="event_logo_id":
#         print("aaa")
