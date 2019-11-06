import sys,getopt
import pymysql
import string
import re

db = pymysql.connect("diy12.haier.com","horus","horus1317","horus",charset='utf8')

newin = []   #层级关系列表
index = 1   #层级索引

#从根部向上查询依赖
def getTree(input_list):
    global index
    list = []    
    cursor = db.cursor()
    if len(input_list) == 0:
        return input_list
    else:
        for i in input_list:
            JOBNAME = re.search(r'([^/]*$)',i[1]).group(1)
            cursor.execute("select tpl_path from horus_oozie_jobs where id in ( select oozie_id from horus_oozie_job_dependencies where job_dependency like (select concat('%#',a.oozie_id, '#%' ) from ( select max(m.oozie_id) as oozie_id from (select oozie_id from horus_oozie_job_dependencies where oozie_id in (select b.id from horus_oozie_tasks a left join horus_oozie_jobs b on a.job_name = b.name where a.name = '" + JOBNAME + "')) m) a))")
            fetchresult = cursor.fetchall()
            if len(fetchresult) == 0:
                continue
            r = 1
            for row in fetchresult:
                list.append([i[0] + str(r) + ".",row[0]])
                r = r + 1

        for j in list:
            newin.append(j)

        index = index + 1
        cursor.close()
        return getTree(list)
 
#从顶部向下查询依赖 
def getRoot(input_list):
    global index
    list = []
    if len(input_list) == 0:
        return input_list
    else:
        for i in input_list:
            JOBNAME = re.search(r'([^/]*$)',i[1]).group(1)
            cursor = db.cursor()
            cursor.execute("select job_dependency from horus_oozie_job_dependencies where oozie_id in (select b.id from horus_oozie_tasks a left join horus_oozie_jobs b on a.job_name = b.name where a.name = '" + JOBNAME + "')")
            if cursor.rowcount == 0:
                continue
            fetchresult = cursor.fetchone()
            job_ids = str(fetchresult[0])
            id_array  = job_ids.split("#")
            inlist = ""
            index = 1
            for x in id_array:
                if x != "" and index < len(id_array)-1:
                    inlist = inlist + x + ","
                else:
                    inlist = inlist + x
                index = index + 1

            #上游依赖
            if inlist == "None" or inlist == '':
                #print(JOBNAME, "None")
                continue
                
            cursor.execute("select tpl_path from horus_oozie_jobs where id in (" + inlist + ")")
            fetchresult = cursor.fetchall()
            cursor.close()
            r=1
            for row in fetchresult:
                list.append([i[0] + str(r) + ".",row[0]])
                r = r + 1

            for j in list:
                newin.append(j)    

        return getRoot(list)
    cursor.close()
    
    
def main(argv): 
    JOBNAME = ""    #任务名
    TABNAME = ""     #表明，暂无用
    FROM = 0         #0为从顶部开始，非0为根部开始
    opts,args = getopt.getopt(sys.argv[1:],'-j:-t:-r')
    for opts,args in opts:
        if opts == '-j' and args != None:
            JOBNAME = args
        elif opts == '-t' and args != None:
            TABNAME = args
        elif opts == '-r':
            FROM = 1
        else:
            print("参数：-j <中文任务名> [-r 查询上级依赖]")
            exit(1)
    
    #打印根节点
    print("." + JOBNAME)
    
    if FROM == 0:
        getRoot([['1.',JOBNAME]])
    else:
        getTree([['1.',JOBNAME]])
    
    db.close()
    
    newin.sort()  #升序排序
    
    tree = []   #画树状图的列表
    for i in range(0,len(newin)):
        r = newin[i][0].split('.')   #按‘.’分割层级代码
        l = len(r) - 3        #减3后的长度是需要的层级
        r = newin[i][1]       #仅保留需要的内容
        branch = l * "    "+"└── " + r       #按层级添加空格，一层4个空格和分枝字符
        tree.append(branch)         
        pos = branch.index('└')       #获取└的索引

        j = i - 1
        while j >= 0:        #倒序寻找上一个元素的相同索引位置
            if tree[j][pos] == '└':      #为└时说明时同一级，替换成上下连接符号
                tree[j] = tree[j][0:pos] + "├" + tree[j][pos+1:]     
            elif tree[j][pos] == ' ':    #为空格时说明为下一级，替换成竖线连接到以连接到同级元素
                tree[j] = tree[j][0:pos] + "│" + tree[j][pos+1:]
            else:
                break
            j = j - 1

    for i in tree:
        print(i)

    
    
if __name__ == "__main__":
    main(sys.argv[1:])
