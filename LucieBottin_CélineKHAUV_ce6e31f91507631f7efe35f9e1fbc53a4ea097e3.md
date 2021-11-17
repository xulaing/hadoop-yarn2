## TP YARN MAP REDUCE

### Céline Khauv | Lucie Bottin

- As we couldn't find a way to put all the functions in the same repository, you can find the last query (District containing the oldest tree (difficult)) on Github
- https://github.com/xulaing/hadoop-yarn2
- commit : ce6e31f91507631f7efe35f9e1fbc53a4ea097e3

## 1.6 Send the JAR to the edge node

## 1.6.3 Run the job

[lucie.bottin@hadoop-edge01 ~]$ yarn jar /home/lucie.bottin/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \wordcount /user/lucie.bottin/davinci.txt /user/lucie.bottin/wordcount

[lucie.bottin@hadoop-edge01 ~]$ hdfs dfs -cat wordcount/part-r-00000

youth. 2
youth.] 1
youth; 1
youthful 3
youwant 1
z 1
z* 2
z*. 2
z*; 1
zeal 1
zelus 1
zenith 2
zerfielen. 1
zero; 1
zum 1
zur 1
zvith 1
zwanzig 1
zweite 1
àpieza; 1
è 3
è: 1
ècrit* 1
• 2

## 1.8 Remarkable trees of Paris

## 1.8.1 Districts containing trees (very easy)

### Mapper

public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
public void map(Object key, Text value, Context context)
throws IOException, InterruptedException {
String arrondissement = value.toString().split(";")[1];
context.write(new Text(arrondissement), new Text(" "));
}
}

### Reducer

public class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, new Text(" "));
    }

}

### Test

@Test
public void testMap() throws IOException, InterruptedException {
String value = "(48.8399672948, 2.43375148978);12;Fagus;sylvatica;Fagaceae;1865;20.0;530.0;avenue Daumesnil, Esplanade du Château de Vincennes;Hêtre pleureur;Pendula;20;Bois de Vincennes (square Carnot)";
this.tokenizerMapper.map(null, new Text(value), this.context);
verify(this.context, times(1))
.write(new Text("12"), new Text(" "));
}

#### We can test it with the real file

[lucie.bottin@hadoop-edge01 ~]$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \query1 /user/lucie.bottin/trees.csv /user/lucie.bottin/query1

21/11/04 10:15:01 INFO mapreduce.Job: map 0% reduce 0%
21/11/04 10:15:10 INFO mapreduce.Job: map 100% reduce 0%
21/11/04 10:15:20 INFO mapreduce.Job: map 100% reduce 100%
21/11/04 10:15:20 INFO mapreduce.Job: Job job_1630864376208_4529 completed successfully
21/11/04 10:15:20 INFO mapreduce.Job: Counters: 54

[lucie.bottin@hadoop-edge01 ~]$ hdfs dfs -ls query1/
Found 2 items
-rw-r--r-- 3 lucie.bottin lucie.bottin 0 2021-11-04 10:15 query1/\_SUCCESS
-rw-r--r-- 3 lucie.bottin lucie.bottin 95 2021-11-04 10:15 query1/part-r-00000

[lucie.bottin@hadoop-edge01 ~]$ hdfs dfs -cat query1/part-r-00000
11
12
13
14
15
16
17
18
19
20
3
4
5
6
7
8
9
ARRONDISSEMENT

## 1.8.2 Show all existing species (very easy)

### Mapper

public class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
public void map(Object key, Text value, Context context)
throws IOException, InterruptedException {
String species = value.toString().split(";")[3];
context.write(new Text(species), new Text(" "));
}
}

#### We can test it with the real file

[celine.khauv@hadoop-edge01 ~]$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \query2 /user/celine.khauv/trees.csv /user/celine.khauv/query2

21/11/04 10:28:21 INFO mapreduce.Job: Running job: job_1630864376208_4539
21/11/04 10:28:31 INFO mapreduce.Job: Job job_1630864376208_4539 running in uber mode : false
21/11/04 10:28:31 INFO mapreduce.Job: map 0% reduce 0%
21/11/04 10:28:40 INFO mapreduce.Job: map 100% reduce 0%
21/11/04 10:28:45 INFO mapreduce.Job: map 100% reduce 100%
21/11/04 10:28:46 INFO mapreduce.Job: Job job_1630864376208_4539 completed successfully
21/11/04 10:28:46 INFO mapreduce.Job: Counters: 54

[celine.khauv@hadoop-edge01 ~]$ hdfs dfs -cat query2/part-r-00000  
ESPECE
araucana
atlantica
australis
baccata
bignonioides
biloba
bungeana
cappadocicum
carpinifolia
colurna
coulteri
decurrens
dioicus
distichum
excelsior
fraxinifolia
giganteum
giraldii
glutinosa
grandiflora
hippocastanum
ilex
involucrata
japonicum
kaki
libanii
monspessulanum
nigra
nigra laricio
opalus
orientalis
papyrifera
petraea
pomifera
pseudoacacia
sempervirens
serrata
stenoptera
suber
sylvatica
tomentosa
tulipifera
ulmoides
virginiana
x acerifolia

## 1.8.3 Number of trees by kinds (easy)

### Mapper

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
private final static IntWritable one = new IntWritable(1);
private Text kind = new Text();
public void map(Object key, Text value, Context context)
throws IOException, InterruptedException {
kind = new Text(value.toString().split(";")[3]);
context.write(kind, one);
}
}

### Reducer

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
private IntWritable result = new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
sum += val.get();
}
result.set(sum);
context.write(key, result);
}
}

#### Test

@Test
public void testMap() throws IOException, InterruptedException {
String value = "(48.8399672948, 2.43375148978);12;Fagus;sylvatica;Fagaceae;1865;20.0;530.0;avenue Daumesnil, Esplanade du Château de Vincennes;Hêtre pleureur;Pendula;20;Bois de Vincennes (square Carnot)";
this.tokenizerMapper.map(null, new Text(value), this.context);
verify(this.context, times(1))
.write(new Text("sylvatica"), new IntWritable(1));
}

@Test
public void testReduce() throws IOException, InterruptedException {
String key = "sylvatica";
IntWritable value = new IntWritable(1);
Iterable<IntWritable> values = Arrays.asList(value, value, value);
this.intSumReducer.reduce(new Text(key), values, this.context);
verify(this.context).write(new Text(key), new IntWritable(3));
}

#### We can test it with the real file

[celine.khauv@hadoop-edge01 ~]$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \query3 /user/celine.khauv/trees.csv /user/celine.khauv/query3

21/11/04 11:16:27 INFO mapreduce.Job: map 0% reduce 0%
21/11/04 11:16:36 INFO mapreduce.Job: map 100% reduce 0%
21/11/04 11:16:45 INFO mapreduce.Job: map 100% reduce 100%
21/11/04 11:16:45 INFO mapreduce.Job: Job job_1630864376208_4558 completed successfully
21/11/04 11:16:45 INFO mapreduce.Job: Counters: 54

[celine.khauv@hadoop-edge01 ~]$ hdfs dfs -cat query3/part-r-00000  
ESPECE 1
araucana 1
atlantica 2
australis 1
baccata 2
bignonioides 1
biloba 5
bungeana 1
cappadocicum 1
carpinifolia 4
colurna 3
coulteri 1
decurrens 1
dioicus 1
distichum 3
excelsior 1
fraxinifolia 2
giganteum 5
giraldii 1
glutinosa 1
grandiflora 1
hippocastanum 3
ilex 1
involucrata 1
japonicum 1
kaki 2
libanii 2
monspessulanum 1
nigra 3
nigra laricio 1
opalus 1
orientalis 8
papyrifera 1
petraea 2
pomifera 1
pseudoacacia 1
sempervirens 1
serrata 1
stenoptera 1
suber 1
sylvatica 8
tomentosa 2
tulipifera 2
ulmoides 1
virginiana 2
x acerifolia 11

## 1.8.4 Maximum height per kind of tree (average)

### Mapper

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable> {
private int line = 0;
public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
if (line != 0){ // Skip Header
String[] fields = value.toString().split(";");
Text kind = new Text(fields[2]); // Get the kind

            Float height = new Float(0);
            try{
                height = Float.parseFloat(fields[6]); // Get its height
            }catch(NumberFormatException ex){}

            context.write(kind, new FloatWritable(height)); // Write both of them in the context
        }
        line++;
    }

}

### Reducer

public class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
public void reduce(Text kind, Iterable<FloatWritable> heights, Context context) throws IOException InterruptedException{
float max = 0;
for(FloatWritable height : heights){
if( height.get() > max){
max = height.get();
}
}
context.write(kind, new FloatWritable(max));
}
}

#### We can test it with the real file

[celine.khauv@hadoop-edge01 ~]$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \query4 /user/celine.khauv/trees.csv /user/celine.khauv/query4

21/11/17 22:02:31 INFO mapreduce.Job: map 0% reduce 0%
21/11/17 22:02:39 INFO mapreduce.Job: map 100% reduce 0%
21/11/17 22:02:44 INFO mapreduce.Job: map 100% reduce 100%
21/11/17 22:02:45 INFO mapreduce.Job: Job job_1637167060931_0059 completed successfully

[celine.khauv@hadoop-edge01 ~]$ hdfs dfs -cat query4/part-r-00000
Acer 16.0
Aesculus 30.0
Ailanthus 35.0
Alnus 16.0
Araucaria 9.0
Broussonetia 12.0
Calocedrus 20.0
Catalpa 15.0
Cedrus 30.0
Celtis 16.0
Corylus 20.0
Davidia 12.0
Diospyros 14.0
Eucommia 12.0
Fagus 30.0
Fraxinus 30.0
Ginkgo 33.0
Gymnocladus 10.0
Juglans 28.0
Liriodendron 35.0
Magnolia 12.0
Paulownia 20.0
Pinus 30.0
Platanus 45.0
Pterocarya 30.0
Quercus 31.0
Robinia 11.0
Sequoia 30.0
Sequoiadendron 35.0
Styphnolobium 10.0
Taxodium 35.0
Taxus 13.0
Tilia 20.0
Ulmus 15.0
Zelkova 30.0

## 1.8.5 - Sort the trees height from smallest to largest

### Mapper

public class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>{
private int line = 0;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (line != 0){ // Skip Header
            try{
                String[] fields = value.toString().split(";");
                Text kind = new Text(fields[11] + " - " + fields[2] + " : "); // Get the kind
                Float height = Float.parseFloat(fields[6]); // Get its height
                context.write(new FloatWritable(height), kind); // Write both of them in the context
            }catch(NumberFormatException ex){}
        }
        line++;
    }

}

### Reducer

public class IntSumReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {

    public void reduce(FloatWritable height, Iterable<Text> kinds, Context context) throws IOException, InterruptedException{
        for(Text kind : kinds){
            context.write(kind, height); // Write the kind and its height
        }
    }

}

#### We can test it with the real file

[celine.khauv@hadoop-edge01 ~]$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \query5 /user/celine.khauv/trees.csv /user/celine.khauv/query5

21/11/17 22:33:40 INFO mapreduce.Job: map 0% reduce 0%
11834 [main] INFO org.apache.hadoop.mapreduce.Job - map 0% reduce 0%
21/11/17 22:33:49 INFO mapreduce.Job: map 100% reduce 0%
20941 [main] INFO org.apache.hadoop.mapreduce.Job - map 100% reduce 0%
21/11/17 22:33:55 INFO mapreduce.Job: map 100% reduce 100%
27000 [main] INFO org.apache.hadoop.mapreduce.Job - map 100% reduce 100%

[celine.khauv@hadoop-edge01 ~]$ hdfs dfs -cat query5/part-r-00000
3 - Fagus : 2.0
89 - Taxus : 5.0
62 - Cedrus : 6.0
39 - Araucaria : 9.0
44 - Styphnolobium : 10.0
32 - Quercus : 10.0
95 - Pinus : 10.0
61 - Gymnocladus : 10.0
63 - Fagus : 10.0
4 - Robinia : 11.0
93 - Diospyros : 12.0
66 - Magnolia : 12.0
50 - Zelkova : 12.0
7 - Eucommia : 12.0
48 - Acer : 12.0
58 - Diospyros : 12.0
33 - Broussonetia : 12.0
71 - Davidia : 12.0
36 - Taxus : 13.0
96 - Pinus : 14.0
94 - Diospyros : 14.0
68 - Diospyros : 14.0
91 - Acer : 15.0
5 - Catalpa : 15.0
70 - Fagus : 15.0
2 - Ulmus : 15.0
98 - Quercus : 15.0
78 - Acer : 16.0
16 - Celtis : 16.0
28 - Alnus : 16.0
75 - Zelkova : 16.0
83 - Zelkova : 18.0
23 - Aesculus : 18.0
64 - Ginkgo : 18.0
60 - Fagus : 18.0
8 - Platanus : 20.0
20 - Fagus : 20.0
87 - Taxodium : 20.0
12 - Sequoiadendron : 20.0
51 - Platanus : 20.0
43 - Tilia : 20.0
35 - Paulownia : 20.0
34 - Corylus : 20.0
15 - Corylus : 20.0
1 - Corylus : 20.0
13 - Platanus : 20.0
11 - Calocedrus : 20.0
86 - Platanus : 22.0
47 - Aesculus : 22.0
14 - Pterocarya : 22.0
88 - Liriodendron : 22.0
10 - Ginkgo : 22.0
18 - Fagus : 23.0
31 - Ginkgo : 25.0
24 - Cedrus : 25.0
84 - Ginkgo : 25.0
92 - Platanus : 25.0
49 - Platanus : 25.0
97 - Pinus : 25.0
73 - Platanus : 26.0
42 - Platanus : 27.0
65 - Pterocarya : 27.0
85 - Juglans : 28.0
52 - Fraxinus : 30.0
29 - Zelkova : 30.0
37 - Cedrus : 30.0
27 - Sequoia : 30.0
25 - Fagus : 30.0
54 - Pterocarya : 30.0
69 - Pinus : 30.0
41 - Platanus : 30.0
77 - Taxodium : 30.0
30 - Aesculus : 30.0
55 - Platanus : 30.0
38 - Fagus : 30.0
76 - Pinus : 30.0
19 - Quercus : 30.0
72 - Sequoiadendron : 30.0
59 - Sequoiadendron : 30.0
22 - Cedrus : 30.0
9 - Platanus : 31.0
80 - Quercus : 31.0
82 - Platanus : 32.0
46 - Ginkgo : 33.0
45 - Platanus : 34.0
53 - Ailanthus : 35.0
17 - Platanus : 35.0
56 - Taxodium : 35.0
81 - Liriodendron : 35.0
57 - Sequoiadendron : 35.0
40 - Platanus : 40.0
26 - Platanus : 40.0
74 - Platanus : 40.0
90 - Platanus : 42.0
21 - Platanus : 45.0

## 1.8.6 District containing the oldest tree (difficult)

### Mapper

public class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
private int line = 0;

    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (line != 0){ // Skip Header
            try{
                String[] fields = value.toString().split(";");
                int district = Integer.parseInt(fields[1]); // Get the kind
                int year = Integer.parseInt(fields[5]); // Get the year
                context.write(new IntWritable(year), new IntWritable(district)); // Write both of them in the context
            }catch(NumberFormatException ex){
            }
        }
        line++;
    }

}

### Reducer

public class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public Boolean first = true;

    public void reduce(IntWritable year, Iterable<IntWritable> districts, Context context) throws IOException,InterruptedException{

        if (first){
            for(IntWritable district : districts){
                try{
                    context.write(year, district);
                }catch(Exception e){

                }
            }
        }
        first = false;
    }

}

#### We can test it with the real file

[celine.khauv@hadoop-edge01 ~]$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar \query6 /user/celine.khauv/trees.csv /user/celine.khauv/query6

21/11/17 22:15:16 INFO mapreduce.Job: map 0% reduce 0%
12592 [main] INFO org.apache.hadoop.mapreduce.Job - map 0% reduce 0%
21/11/17 22:15:24 INFO mapreduce.Job: map 100% reduce 0%
20701 [main] INFO org.apache.hadoop.mapreduce.Job - map 100% reduce 0%
21/11/17 22:15:33 INFO mapreduce.Job: map 100% reduce 100%
29789 [main] INFO org.apache.hadoop.mapreduce.Job - map 100% reduce 100%
21/11/17 22:15:33 INFO mapreduce.Job: Job job_1637167060931_0062 completed successfully
29809 [main] INFO org.apache.hadoop.mapreduce.Job - Job job_1637167060931_0062 completed successfully

[celine.khauv@hadoop-edge01 ~]$ hdfs dfs -cat query6/part-r-00000
1601 5
