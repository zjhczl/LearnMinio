import oss
from minio.commonconfig import REPLACE, CopySource

client = oss.getClient()
bucketname = "3dtiles"
# pullFiles(client, bucketname, projectName, filesDir)
# print(getFilesNum(client, bucketname, projectName))
# oss.pushProject(client, "droneimg", "D:/ss", "yishankouciyao")

# oss.cloneProject(client, bucketname, projectName, filesDir)
oss.pushProject(client, "3dtiles", "D:/CCProject/root", "tileset.json")
# copy
# print("---")
# oss.upLoadProject(client, bucketname, "D:/CCProject/Production", "yishankou")
# print("---")
oss.pushProject(client, "droneimg", "D:/CCProject/img", "zaozhuang")