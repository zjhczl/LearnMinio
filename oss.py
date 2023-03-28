from minio import Minio
import os
from minio.commonconfig import REPLACE, CopySource
import time


def getTimeStr():
    return time.strftime("%Y%m%d", time.localtime())


def getObjNameWithTime(objName):
    parts = objName.replace("\\", "/").replace("//", "/").split("/")
    parts[0] = parts[0] + "/" + getTimeStr()
    return "/".join(parts)


def getClient(endpoint="192.168.1.50:9000",
              access_key='minioadmin',
              secret_key='minioadmin',
              region="ch-sd"):
    MINIO_CONF = {
        'endpoint': endpoint,
        'access_key': access_key,
        'secret_key': secret_key,
        "region": region,
        'secure': False
    }
    client = Minio(**MINIO_CONF)
    return client


def getObjects(client, bucketname, prefix):
    objects = client.list_objects(bucketname, prefix=prefix, recursive=True)
    return objects


# 克隆bucket的项目到本地,不分时间
def cloneProject(client, bucketname, projectName, filesDir):
    objects = client.list_objects(bucketname,
                                  prefix=projectName,
                                  recursive=True)
    for obj in objects:
        parts = obj.object_name.split("/")
        parts[1] = '/'
        localkey = "/".join(parts).replace("//", "/")

        print(bucketname + ":" + obj.object_name + "-->" + filesDir + "/" +
              localkey)
        client.fget_object(bucketname, obj.object_name,
                           filesDir + "/" + localkey)


# 从bucket下载文件到本地
def downLoadProjects(client, bucketname, filesDir):
    objects = client.list_objects(bucketname, prefix='\\', recursive=True)
    for obj in objects:
        print(bucketname + ":" + obj.object_name + "-->" + filesDir + "/" +
              obj.object_name)
        client.fget_object(bucketname, obj.object_name,
                           filesDir + "/" + obj.object_name)


# 从bucket下载文件到本地
def downLoadProject(client, bucketname, prefix, filesDir):
    objects = client.list_objects(bucketname, prefix=prefix, recursive=True)
    for obj in objects:
        print(bucketname + ":" + obj.object_name + "-->" + filesDir + "/" +
              obj.object_name)
        client.fget_object(bucketname, obj.object_name,
                           filesDir + "/" + obj.object_name)


# 上传文件到bucket
def upLoadProject(client, bucketname, filesDir, projectName):
    for path, dirnames, filenames in os.walk(filesDir):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = path.replace(filesDir, '')
        # print(fpath[1:].split("\\"))
        if (projectName == fpath[1:].split("\\")[0]):
            for filename in filenames:
                key = fpath + "\\" + filename
                key = getObjNameWithTime(key[1:])
                client.fput_object(bucketname, key, path + "/" + filename)
                print(path + "/" + filename + "-->" + bucketname + ":" + key)


# 上传文件到bucket
def upLoadProjects(client, bucketname, filesDir):
    for path, dirnames, filenames in os.walk(filesDir):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = path.replace(filesDir, '')

        for filename in filenames:
            key = fpath + "\\" + filename
            key = getObjNameWithTime(key[1:])
            client.fput_object(bucketname, key, path + "/" + filename)
            print(path + "/" + filename + "-->" + bucketname + ":" + key)


# 删除一个对象
# Remove object.
def deleteObject(client, bucketname, key):
    client.remove_object(bucketname, key)


def getFilesNum(client, bucketname, prefix):
    objects = client.list_objects(bucketname + "-cache",
                                  prefix=prefix,
                                  recursive=True)
    return len(list(objects))


# 拉取project从cache到本地
def pullProjects(client, bucketname, filesDir):
    bucketnameCache = bucketname + "-cache"
    objects = client.list_objects(bucketnameCache, prefix='/', recursive=True)
    for obj in objects:
        objName = obj.object_name
        objNameWithTime = getObjNameWithTime(objName)
        print(bucketnameCache + ":" + objName + "-->" + filesDir + "/" +
              objName)
        # 下载文件到本地
        client.fget_object(bucketnameCache, objName, filesDir + "/" + objName)

        # copy
        client.copy_object(
            bucketname,
            objNameWithTime,
            CopySource(bucketnameCache, objName),
        )

        # 删除原对象
        client.remove_object(bucketnameCache, objName)


# 拉取project从cache到本地
def pullProject(client, bucketname, prefix, filesDir):
    bucketnameCache = bucketname + "-cache"
    objects = client.list_objects(bucketnameCache,
                                  prefix=prefix,
                                  recursive=True)
    for obj in objects:
        objName = obj.object_name
        objNameWithTime = getObjNameWithTime(objName)
        print(bucketnameCache + ":" + objName + "-->" + filesDir + "/" +
              objName)
        # 下载文件到本地
        client.fget_object(bucketnameCache, objName, filesDir + "/" + objName)

        # copy
        client.copy_object(
            bucketname,
            objNameWithTime,
            CopySource(bucketnameCache, objName),
        )

        # 删除原对象
        client.remove_object(bucketnameCache, objName)


# push本地文件到cache
def pushProject(client, bucketname, filesDir, projectName):
    bucketnameCache = bucketname + "-cache"
    if ("." in projectName):

        client.fput_object(bucketnameCache, '/' + projectName,
                           filesDir + "/" + projectName)
        print(filesDir + "/" + projectName + "--> " + bucketnameCache + ":" +
              '/' + projectName)
    else:
        for path, dirnames, filenames in os.walk(filesDir):
            # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
            fpath = path.replace(filesDir, '')
            if (projectName == fpath[1:].split("\\")[0]):
                for filename in filenames:
                    key = fpath + "\\" + filename
                    print(path + "/" + filename + "-->" + bucketnameCache +
                          ":" + key)

                    client.fput_object(bucketnameCache, key,
                                       path + "/" + filename)


# push本地文件到cache
def pushProjects(client, bucketname, projectsDir):
    bucketnameCache = bucketname + "-cache"
    for path, dirnames, filenames in os.walk(projectsDir):
        # 去掉目标跟路径，只对目标文件夹下边的文件及文件夹进行压缩
        fpath = path.replace(projectsDir, '')
        for filename in filenames:
            key = fpath + "\\" + filename
            print(path + "/" + filename + "-->" + bucketnameCache + ":" + key)
            client.fput_object(bucketnameCache, key, path + "/" + filename)


# c = getClient()
# pushFiles(c, "3dtiles", "D:/cx/3dtiles", "yishankou")
