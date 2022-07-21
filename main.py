import csv
import math
import os
from multiprocessing import Pool, freeze_support, cpu_count

import Levenshtein

groupSize = 1
padding = 0

columnToCompare = 0
percent = 0.8

def calc(group1, group2, start, end):
    totalRes = []

    #print(start)
    completed = 0

    for group1ItemIndex in range(start, end):
        group1Item = group1[group1ItemIndex]
        maxItem = []
        maxValue = 0
        res = group1Item[0:-1]
        for group2Item in group2:
            ratio = Levenshtein.ratio(group1Item[groupSize], group2Item[groupSize])
            if ratio > percent and maxValue < ratio:
                maxItem = group2Item
                maxValue = ratio

        if len(maxItem) > 0:
            res.extend(maxItem[0:-1])
        else:
            res.append("не найдено")

        completed = completed + 1
        print(completed / (end - start))

        totalRes.append(res)

    return totalRes


def calc_wrap(args):
    return calc(*args)

if __name__ == '__main__':
    freeze_support()
    with open('task.csv', newline='', encoding="utf8") as csvfile:
        reader = csv.reader(csvfile, delimiter=";")

        heading = next(csvfile).split(";")


        group1 = []
        group2 = []

        for row in reader:
            if groupSize == 1:
                group1Item = [row[0]]
                if len(group1Item) > 0:
                    group1Item.append(group1Item[columnToCompare].lower())
                    group1.append(group1Item)

                group2Item = [row[groupSize + padding]]
                if group2Item[columnToCompare] != "":
                    group2Item.append(group2Item[columnToCompare].lower())
                    group2.append(group2Item)
            else:
                group1Item = row[0: groupSize]
                if len(group1Item) > 0:
                    group1Item.append(group1Item[columnToCompare].lower())
                    group1.append(group1Item)

                group2Item = row[groupSize + padding: groupSize * 2 + padding]
                if len(group2Item) > 0:
                    group2Item.append(group2Item[columnToCompare].lower())
                    group2.append(group2Item)

    all_args = []

    cpuCount = cpu_count()
    length = len(group1)
    part = math.floor(length / cpuCount)
    for i in range(0, cpuCount):
        start = part * i
        all_args.append([group1, group2, start, start + part])

    all_args[cpuCount - 1][3] = length

    if os.name == "nt":
        freeze_support()

    pool = Pool(cpu_count())

    results = pool.map(calc_wrap, all_args)

    totalRes = []
    for res in results:
        totalRes.extend(res)

    with open('res.csv', 'w', newline='', encoding="utf8") as rescsv:
        writer = csv.writer(rescsv, delimiter=';')
        for res in totalRes:
            writer.writerow(res)

