from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("/Users/sofia/Projects/workspace/courses/udemy/taming-big-data-with-apache-spark/data/customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x+y)

flipped = totalByCustomer.map(lambda (x,y):(y,x))
totalByCustomerSorted = flipped.sortByKey(False)

results = totalByCustomerSorted.collect();
for result in results:
    amount, customer = result
    amount = round(amount, 2)
    print "%s %s" % (amount, customer)
