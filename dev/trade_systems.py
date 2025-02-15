import os
from hikyuu import * 
from hikyuu.draw import *

erdir = '/home/chuazhou/nginxhku/render/' # echart_render_root

def hkuplt_echarts(pfunc, *args):
    use_draw_engine('echarts')
    kc, sfile = pfunc(*args)
    kc.render(f'{erdir}{sfile}.html')


def draw_stock_kline(code, *args):
    kdata = sm[code].get_kdata(Query(0))
    ma5 = MA(C(kdata), 5)
    ma5.name = 'MA5'
    ma10 = MA(C(kdata), 10)
    ma10.name = 'MA10'
    ma20 = MA(C(kdata), 20)
    ma20.name = 'MA20'
    v5 = MA(VOL(kdata), 5)
    v5.name = 'VOL5'
    v10 = MA(VOL(kdata), 10)
    v10.name = 'VOL10'
    return kdata.plot([ma5, ma10, ma20], [v5, v10]), code+sm[code].name

if __name__ == '__main__':
    hikyuu_init(os.path.expanduser('~/.hikyuu/hikyuu.ini'))
    sc = ['sh880001', 'sh880002', 'sh880003', 'sh000001']
    for s in sc:
        hkuplt_echarts(draw_stock_kline, s)
