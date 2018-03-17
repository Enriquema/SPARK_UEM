# SPARK_UEM

Este documento detalla la información relativa al código desarrollado por el alumno Enrique Mora Alonso y los problemas que ha encontrado. Para la descripción del código y lo que hacen las partes del mismo se detallará el código punto a punto.


En primer lugar  la clase scala importa una serie de paquetes imprescindibles para el uso de Spark como:

	import org.apache.spark._
	import org.apache.spark.sql.SQLContext

Seguidamente, e independientemente del control de parámetros, nuestra clase implementa las siguientes líneas:

	val sparkConf = new SparkConf().setAppName("StackOverflow")
	val sc = new SparkContext(sparkConf)
	val sqlContext = new SQLContext(sc)

Estas definen el SparkContext y SQLContext mediante los cuales leeremos la información de las fuentes de datos como veremos a continuación.

Una vez tenemos configurados nuestros context, hacemos la carga de la información que vamos a procesar, para ello utilizamos la siguiente línea de código:

	val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "row").load("hdfs:////stackoverflow/Users.xml")

Esta nos permite guardar en una variable “df” el dataframe cargado de los usuarios de Stackoverflow que tenemos almacenado en nuestra unidad de HDFS. Como los ficheros que se leen son del tipo XML, es importante especificar que se va a utilizar la librería “com.databricks.spark.xml” para poder procesar las etiquetas implícitas de XML y la información contenida en ellas.

Esta operación puede tardar unos minutos y como no queremos tener que cargar esta información cada vez que queramos realizar una consulta, utilizaremos la siguiente línea de código para almacenar la información en caché y así optimizar la lectura de datos:

	df.cache

A continuación registraremos la información cargada como una tabla para poder acceder a la información siempre y cuando queramos. Para ello ejecutaremos la línea:

	df.registerTempTable("StackOverflow")

Una vez llegados a este punto es importante que podemos visualizar el esquema de la información cargada mediante el comando:

	df.registerTempTable("StackOverflow")

Esto nos proporcionará la información en detalle de los campos que existen en el dataframe y el tipo de dato de cada uno de ellos como se puede ver:

SCHEMA
 |-- _AboutMe: string (nullable = true)
 |-- _AccountId: long (nullable = true)
 |-- _Age: long (nullable = true)
 |-- _CreationDate: string (nullable = true)
 |-- _DisplayName: string (nullable = true)
 |-- _DownVotes: long (nullable = true)
 |-- _Id: long (nullable = true)
 |-- _LastAccessDate: string (nullable = true)
 |-- _Location: string (nullable = true)
 |-- _ProfileImageUrl: string (nullable = true)
 |-- _Reputation: long (nullable = true)
 |-- _UpVotes: long (nullable = true)
 |-- _Views: long (nullable = true)
 |-- _WebsiteUrl: string (nullable = true)
 |-- empty: string (nullable = true)

Analizando el esquema se puede contemplar que los datos pertenecen a comentarios de los usuarios de stackoverflow y que se puede sacar la infomación de los usuarios que los ponene, edad de los mismos, votos positivos de los comentarios…


Una vez que tenemos la tabla “stackoverflow” registrada vamos a proceder a consultar la información.

	val query = sqlContext.sql("SELECT IF(_AboutMe='' OR _AboutMe is null,'-100',_AboutMe) AS _AboutMe, IF(_AccountId='' OR _AccountId is null,'-100',_AccountId) AS _AccountId, IF(_Age='' OR _Age is null,'-100',_Age) AS _Age, IF(_CreationDate='' OR _CreationDate is null,'-100',_CreationDate) AS _CreationDate, IF(_DisplayName='' OR _DisplayName is null,'-100',_DisplayName) AS _DisplayName, IF(_DownVotes='' OR _DownVotes is null,'-100',_DownVotes) AS _DownVotes, IF(_Id='' OR _Id is null,'-100',_Id) AS _Id, IF(_LastAccessDate='' OR _LastAccessDate is null,'-100',_LastAccessDate) AS _LastAccessDate, IF(_Location='' OR _Location is null,'-100',_Location) AS _Location, IF(_ProfileImageUrl='' OR _ProfileImageUrl is null,'-100',_ProfileImageUrl) AS _ProfileImageUrl, IF(_Reputation='' OR _Reputation is null,'-100',_Reputation) AS _Reputation, IF(_UpVotes='' OR _UpVotes is null,'-100',_UpVotes) AS _UpVotes, IF(_Views='' OR _Views is null,'-100',_Views) AS _Views, IF(_WebsiteUrl='' OR _WebsiteUrl is null,'-100',_WebsiteUrl) AS _WebsiteUrl FROM StackOverflow")

Mediante la anterior sentencia podremos obtener todos los valores del SCHEMA mostrado en la parte anterior y además pondremos un valor por defecto para aquellos campos Nulos o vacíos, de modo que todo valor Nulo o vacío sea “-100” y evitemos posibles NULLPOINTEREXCEPTION.

Una vez transformada la información, pasaremos a realizar un RDD para trabajar con la información de una manera más eficiente.

	val rdd = query.rdd



EJERCICIOS REALIZADOS CON RDD

En primer lugar se ha realizado un ejercicio de comparativa en el que se ejecuta un contador de los registros de nuestro dataset con un repartition y sin él para experimentar que mejora nos porporciona el repartition.
	
	val timeStart = System.nanoTime
	rdd.repartition(10).count
	val timeStop = System.nanoTime
	// Tiempo en segundos
	val timeQuery = (timeStop - timeStart) / 1e9d

	// Ejemplo sin rebalanceo de carga para compara tiempos
	val timeStart1 = System.nanoTime
	rdd.count
	val timeStop1 = System.nanoTime
	// Tiempo en segundos
	val timeQuery1 = (timeStop1 - timeStart1) / 1e9d

Este ejemplo contempla una serie de variables similares para poder medir los tiempos de ejecución de las acciones. Como curiosidad, el ejemplo es más óptimo para la parte que no utiliza el “repartition” para el ejemplo del 10% del dataset. Por otro lado, respecto al dataset original, no he sido capaz de ejecutar el “.jar” debido a problemas de memoria, por lo que no podría decir si realmente existe mejoría de la acción con repartition frente a la opción sin repartition.


En segundo lugar, he realizado un análisis del SCHEMA mostrado inicialmente y tras contrastar que varios datos pertenecen o podrían pertenecer a personas físicas, he optado por crear una clase Persona con los atributos que, a mi parecer, podrían pertenecer a personas físicas. Para ello hemos creado una clase Persona y le hemos especificado que serie de parámetros debe tener y de que tipo. Una vez realizado esto se ha obtenido un RDD de Personas con los parámteros especificados del RDD inicial mediante las líneas de código:

	// Se crea una clase Persona que contenga los parametros: _AccountId,_DisplayName,_AboutMe,_Age,_Location,_ProfileImageUrl,_Reputation
	case class Person(id:Int,name:String,aboutMe:String,age:Int,location:String,imageUrl:String,reputation:Int)
	// Se crea un RDD que contenga a las personas de los mensajes cargados
	val peopleRDD = rdd.map(x => Person(x(1).toString.toInt,x(4).toString,x(0).toString,x(2).toString.toInt,x(8).toString,x(9).toString,x(10).toString.toInt))

Como lo anterior tan solo era una transformación y Spark es perezoso por defecto, a continuación se ha explotado la información de ese RDD de Personas nuevo para obtener que nombres son los más repetidos. Esto se ha realizado mediante una selección de los nombre y una asignación de 1 para cada uno de ellos y una reducción y ordenación posterior como se puede ver.

	// Se calculan los nombre más repetidos
	val personName = peopleRDD.map(x => (x.name, 1L)).reduceByKey(_+_)

	val timeStart2 = System.nanoTime
	val personNameCount = personName.collect().sortBy(wc => -wc._2)
	val timeStop2 = System.nanoTime
	// Tiempo en segundos
	val timeQuery2 = (timeStop2 - timeStart2) / 1e9d
	personNameCount.take(10).foreach(println)

Al igual que en los anteriores ejemplos, se mide el tiempo de ejecución de la acción para contemplar cuanto tiempo ha tardado en ejecutarse nuestra acción.


Como otros ejemplos de uso del RDD se realizan 2 acciones más , estas obtienen el número de personas que tienen una edad de entre 10 y 20 años y además el nombre de estas personas mediante las siguientes líneas de código:

	// Obtenemos el número de personas que tienen una edad entre 10 y 20 años
	val timeStart3 = System.nanoTime
	val teenagerCount = peopleRDD.repartition(25).filter(x => x.age > 10).filter(x => x.age < 20).count
	val timeStop3 = System.nanoTime
	// Tiempo en segundos
	val timeQuery3 = (timeStop3 - timeStart3) / 1e9d

	// Otenemos las personas que se encuentran en esa franja de edad y las mostramos.
	val timeStart4 = System.nanoTime
	val teenagers = peopleRDD.repartition(25).filter(x => x.age > 10).filter(x => x.age < 20).map(x => x.name).collect().foreach(println)
	val timeStop4 = System.nanoTime
	// Tiempo en segundos
	val timeQuery4 = (timeStop4 - timeStart4) / 1e9d

Como venimos haciendo a lo largo del código, medimos los tiempos de ejecución de las acciones.
ERROR DE EJECUCIÓN
