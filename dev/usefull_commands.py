from hikyuu import *

# sh880001, 总市值
# sh880002, 流通市值
# sh880003, 平均股价


[s for s in sm if s.type == constant.STOCKTYPE_INDEX and '全' in s.name]

for x in [s for s in sm if s.code.startswith('88')]: print(x)