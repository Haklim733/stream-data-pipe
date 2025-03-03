import sys

sys.path.append('/opt/spark/src/kafka')
sys.path.append('/opt/spark/src/flink')

print("Updated sys.path:")
for p in sys.path:
    print(p)