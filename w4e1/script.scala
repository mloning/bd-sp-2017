//either  run in command line using spark-shell -i file.scala with System.exit(0) at the end, or within spark-shell use :load full-path-to-file.scala
val heathrowFile = sc.textFile("hdfs:///user/user301/temperatureData/heathrowdata.txt")
heathrowFile.take(10).foreach(println)

def isHeader(line: String): Boolean = {
line.contains("yyyy")
}

case class TemperatureRecord(year: Long, month: Int, tmax: Float, tmin: Float, rain: Float)

def parse(line: String) = {
val pieces = line.split('\t')
val year = pieces(0).toLong
val month = pieces(1).toInt
val tmax = pieces(2).toFloat
val tmin = pieces(3).toFloat
val rain = pieces(4).toFloat
TemperatureRecord(year, month, tmax, tmin, rain)
} 

val heathrowData = heathrowFile.filter(x => !isHeader(x)).map(parse)

heathrowData.take(10).foreach(println)

def isMissing(line: String) = {
line.contains("---")
}

val wickairportFile = sc.textFile("hdfs:///user/user301/temperatureData/wickairportdata.txt")

val wickairportData = wickairportFile.filter(x => !isHeader(x)).filter(x => !isMissing(x)).map(parse)

wickairportData.take(10).foreach(println)

heathrowData.sortBy(_.month).take(10).foreach(println)


val heathrowMonthFah = heathrowData.map(x => {
val tmax = x.tmax * 9/5 + 32
val tmin = x.tmin * 9/5 + 32
TemperatureRecord(x.year, x.month, tmax, tmin, x.rain)
}
)

heathrowMonthFah.take(10).foreach(println)

val wickairportRainMM = wickairportData.map(x => {
val rain = x.rain * 10
TemperatureRecord(x.year, x.month, x.tmax, x.tmin, rain)
} 
)

wickairportRainMM.sortBy(_.tmax,false).take(10).foreach(println)




//System.exit(0)
