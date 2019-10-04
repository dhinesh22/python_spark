def analyze(sc, **kwargs):
    """To calculate occurrence of a word in a file"""
    my_file = sc.textFile(kwargs.get('input_path'))
    counts = my_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda v1,v2: v1 + v2)
    counts.saveAsTextFile(kwargs.get('output_path'))
    sc.stop()