import os
from hikyuu import * 
from hikyuu.draw import *

erdir = '/home/chuazhou/nginxhku/render/' # echart_render_root

def hkuplt_echarts():
    use_draw_engine('echarts')
    kldata = sm['sh603040'].get_kdata(Query(0))
    kc = kldata.plot()
    kc.render(erdir + 'sh603040.html')


if __name__ == '__main__':
    hikyuu_init(os.path.expanduser('~/.hikyuu/hikyuu.ini'))
    hkuplt_echarts()
    print('hhh')
