import org.apache.spark._
import org.apache.spark.sql.SQLContext

object StackOverflow {

  /** Usage: StackOverflow [file] */
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: StackOverflow <file>")
      System.exit(1)
    }
	// Se define el nombre de la aplicacion
	val sparkConf = new SparkConf().setAppName("StackOverflow")
	val sc = new SparkContext(sparkConf)

	// Se lee la informacion de StackOverflow
	val sqlContext = new SQLContext(sc)
	val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "row").load("hdfs:////stackoverflow/Users.xml")

	// Se almacena la informacion en la cache para mejorar el rendimiento
	df.cache
	/*// Se imprime el esquema de la informacion extraida para ver como son, este paso es opcional
	df.printSchema*/
	// Se registra la tabla para poder extraer la informacion
	df.registerTempTable("StackOverflow")
		
	// Se transformar la informacion Nula o vacía por un "-100"
	val query = sqlContext.sql("SELECT IF(_AboutMe='' OR _AboutMe is null,'-100',_AboutMe) AS _AboutMe, IF(_AccountId='' OR _AccountId is null,'-100',_AccountId) AS _AccountId, IF(_Age='' OR _Age is null,'-100',_Age) AS _Age, IF(_CreationDate='' OR _CreationDate is null,'-100',_CreationDate) AS _CreationDate, IF(_DisplayName='' OR _DisplayName is null,'-100',_DisplayName) AS _DisplayName, IF(_DownVotes='' OR _DownVotes is null,'-100',_DownVotes) AS _DownVotes, IF(_Id='' OR _Id is null,'-100',_Id) AS _Id, IF(_LastAccessDate='' OR _LastAccessDate is null,'-100',_LastAccessDate) AS _LastAccessDate, IF(_Location='' OR _Location is null,'-100',_Location) AS _Location, IF(_ProfileImageUrl='' OR _ProfileImageUrl is null,'-100',_ProfileImageUrl) AS _ProfileImageUrl, IF(_Reputation='' OR _Reputation is null,'-100',_Reputation) AS _Reputation, IF(_UpVotes='' OR _UpVotes is null,'-100',_UpVotes) AS _UpVotes, IF(_Views='' OR _Views is null,'-100',_Views) AS _Views, IF(_WebsiteUrl='' OR _WebsiteUrl is null,'-100',_WebsiteUrl) AS _WebsiteUrl FROM StackOverflow")
	// Se pasa a RDD
	val rdd = query.rdd

	// Ejemplo con rebalanceo de carga y prueba de memoria
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


	// Se crea una clase Persona que contenga los parametros: _AccountId,_DisplayName,_AboutMe,_Age,_Location,_ProfileImageUrl,_Reputation
	case class Person(id:Int,name:String,aboutMe:String,age:Int,location:String,imageUrl:String,reputation:Int)
	// Se crea un RDD que contenga a las personas de los mensajes cargados
	val peopleRDD = rdd.map(x => Person(x(1).toString.toInt,x(4).toString,x(0).toString,x(2).toString.toInt,x(8).toString,x(9).toString,x(10).toString.toInt))

	// Se calculan los nombre más repetidos
	val personName = peopleRDD.map(x => (x.name, 1L)).reduceByKey(_+_)

	val timeStart2 = System.nanoTime
	val personNameCount = personName.collect().sortBy(wc => -wc._2)
	val timeStop2 = System.nanoTime
	// Tiempo en segundos
	val timeQuery2 = (timeStop2 - timeStart2) / 1e9d
	personNameCount.take(10).foreach(println)

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

	sc.stop()
  }
}
