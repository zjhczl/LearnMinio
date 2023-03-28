import oss
import time

client = oss.getClient("ad.breton.top:8890")
print("启动成功！")
while 1:
    try:

        # 3dtiles 自动更新
        oss.pullProjects(client, "3dtiles",
                         "/data/SGDownload/export/zj/3dtiles")

        # dom 自动更新
        oss.pullProjects(client, "dom", "/data/SGDownload/export/zj/dom")

        # map 自动更新
        oss.pullProject(client, "map", "mapwgs84",
                        "/data/SGDownload/export/zj")

        # dem 自动更新
        oss.pullProject(client, "dem", "dem", "/data/SGDownload/export/zj")

        time.sleep(10)
    except Exception:
        print("拉取失败,重新连接服务... ")
        time.sleep(10)
