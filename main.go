package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

type ETLJobRequest struct {
	JobName     string         `json:"jobName"`
	Source      DataSource     `json:"source"`
	Transforms  []Transform    `json:"transforms,omitempty"`
	Target      *Target        `json:"target,omitempty"`
}

type DataSource struct {
	Type        string `json:"type"`
	AccessKey   string `json:"access_key,omitempty"`
	SecretKey   string `json:"secret_key,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	Path        string `json:"path,omitempty"`
	Format      string `json:"format,omitempty"`
	EndpointURL string `json:"endpoint_url,omitempty"`
	Host        string `json:"host,omitempty"`
	Port        int    `json:"port,omitempty"`
	Database    string `json:"database,omitempty"`
	Table       string `json:"table,omitempty"`
	User        string `json:"user,omitempty"`
	Password    string `json:"password,omitempty"`
}

type Target struct {
	Type        string `json:"type"`
	AccessKey   string `json:"access_key,omitempty"`
	SecretKey   string `json:"secret_key,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	Path        string `json:"path,omitempty"`
	Format      string `json:"format,omitempty"`
	EndpointURL string `json:"endpoint_url,omitempty"`
	Host        string `json:"host,omitempty"`
	Port        int    `json:"port,omitempty"`
	Database    string `json:"database,omitempty"`
	Table       string `json:"table,omitempty"`
	Mode        string `json:"mode,omitempty"`
	User        string `json:"user,omitempty"`
	Password    string `json:"password,omitempty"`
}

type Transform struct {
	Type       string                 `json:"type"`
	Parameters map[string]interface{} `json:"parameters"`
}

func generatePySparkScript(job ETLJobRequest) string {
	var b strings.Builder

	// Import statements
	b.WriteString("from pyspark.sql import SparkSession\n")
	b.WriteString("from pyspark.sql.functions import *\n")
	b.WriteString("from pyspark.sql.types import *\n")
	b.WriteString("\n")

	// Create Spark Session
	b.WriteString(fmt.Sprintf("spark = SparkSession.builder.appName(\"%s\").getOrCreate()\n\n", job.JobName))

	// Generate source reading code
	b.WriteString(generateSourceCode(job.Source))
	b.WriteString("\n")

	// Generate transformation code
	if len(job.Transforms) > 0 {
		b.WriteString("# Apply transformations\n")
		b.WriteString(generateTransformCode(job.Transforms))
		b.WriteString("\n")
	} else {
		b.WriteString("# No transformations applied\n")
		b.WriteString("df_final = df\n\n")
	}

	// Generate target writing code
	if job.Target != nil {
		b.WriteString(generateTargetCode(*job.Target))
	}

	b.WriteString("\nspark.stop()\n")
	return b.String()
}

func generateSourceCode(source DataSource) string {
	var b strings.Builder

	switch source.Type {
	case "s3", "s3_compatible":
		// S3 Configuration
		b.WriteString(fmt.Sprintf("# S3 Configuration\n"))
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.access.key\", \"%s\")\n", source.AccessKey))
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.secret.key\", \"%s\")\n", source.SecretKey))
		
		if source.Type == "s3_compatible" {
			b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.endpoint\", \"%s\")\n", source.EndpointURL))
			b.WriteString("spark._jsc.hadoopConfiguration().set(\"fs.s3a.path.style.access\", \"true\")\n")
		}
		
		path := fmt.Sprintf("s3a://%s/%s", source.Bucket, source.Path)
		b.WriteString(fmt.Sprintf("\npath = \"%s\"\n", path))
		
		// Read based on format
		switch source.Format {
		case "csv":
			b.WriteString("df = spark.read.csv(path, header=True, inferSchema=True)\n")
		case "json":
			b.WriteString("df = spark.read.json(path, multiLine=True)\n")
		case "parquet":
			b.WriteString("df = spark.read.parquet(path)\n")
		case "orc":
			b.WriteString("df = spark.read.orc(path)\n")
		default:
			b.WriteString("df = spark.read.csv(path, header=True, inferSchema=True)\n")
		}

	case "postgresql":
		jdbcURL := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", source.Host, source.Port, source.Database)
		b.WriteString(fmt.Sprintf("# PostgreSQL Configuration\n"))
		b.WriteString(fmt.Sprintf("jdbc_url = \"%s\"\n", jdbcURL))
		b.WriteString("properties = {\n")
		b.WriteString(fmt.Sprintf("    \"user\": \"%s\",\n", source.User))
		b.WriteString(fmt.Sprintf("    \"password\": \"%s\",\n", source.Password))
		b.WriteString("    \"driver\": \"org.postgresql.Driver\"\n")
		b.WriteString("}\n")
		b.WriteString(fmt.Sprintf("table_name = \"%s\"\n", source.Table))
		b.WriteString("df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties)\n")
	}

	return b.String()
}

func generateTransformCode(transforms []Transform) string {
	var b strings.Builder
	
	b.WriteString("df_transformed = df\n\n")
	
	for i, transform := range transforms {
		b.WriteString(fmt.Sprintf("# Transform %d: %s\n", i+1, transform.Type))
		
		switch transform.Type {
		case "filter":
			if condition, ok := transform.Parameters["condition"].(string); ok {
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.filter(\"%s\")\n", condition))
			}
			
		case "select":
			if columns, ok := transform.Parameters["columns"].([]interface{}); ok {
				columnList := make([]string, len(columns))
				for i, col := range columns {
					columnList[i] = fmt.Sprintf("\"%s\"", col.(string))
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.select(%s)\n", strings.Join(columnList, ", ")))
			}
			
		case "rename_column":
			if oldName, ok1 := transform.Parameters["old_name"].(string); ok1 {
				if newName, ok2 := transform.Parameters["new_name"].(string); ok2 {
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.withColumnRenamed(\"%s\", \"%s\")\n", oldName, newName))
				}
			}
			
		case "drop_column":
			if columns, ok := transform.Parameters["columns"].([]interface{}); ok {
				for _, col := range columns {
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.drop(\"%s\")\n", col.(string)))
				}
			}
			
		case "add_column":
			if columnName, ok1 := transform.Parameters["column_name"].(string); ok1 {
				if expression, ok2 := transform.Parameters["expression"].(string); ok2 {
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.withColumn(\"%s\", %s)\n", columnName, expression))
				}
			}
			
		case "join":
			if joinType, ok1 := transform.Parameters["join_type"].(string); ok1 {
				if joinCondition, ok2 := transform.Parameters["join_condition"].(string); ok2 {
					if rightTable, ok3 := transform.Parameters["right_table"].(string); ok3 {
						b.WriteString(fmt.Sprintf("# Note: %s should be loaded separately\n", rightTable))
						b.WriteString(fmt.Sprintf("df_transformed = df_transformed.join(%s, %s, \"%s\")\n", rightTable, joinCondition, joinType))
					}
				}
			}
			
		case "groupby":
			if groupByCols, ok1 := transform.Parameters["group_by"].([]interface{}); ok1 {
				if aggFunctions, ok2 := transform.Parameters["aggregations"].(map[string]interface{}); ok2 {
					groupByList := make([]string, len(groupByCols))
					for i, col := range groupByCols {
						groupByList[i] = fmt.Sprintf("\"%s\"", col.(string))
					}
					
					var aggList []string
					for column, function := range aggFunctions {
						aggList = append(aggList, fmt.Sprintf("%s(\"%s\")", function.(string), column))
					}
					
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.groupBy(%s).agg(%s)\n", 
						strings.Join(groupByList, ", "), strings.Join(aggList, ", ")))
				}
			}
			
		case "sql":
			if query, ok := transform.Parameters["query"].(string); ok {
				if tempView, ok2 := transform.Parameters["temp_view"].(string); ok2 {
					b.WriteString(fmt.Sprintf("df_transformed.createOrReplaceTempView(\"%s\")\n", tempView))
					b.WriteString(fmt.Sprintf("df_transformed = spark.sql(\"\"\"%s\"\"\")\n", query))
				}
			}
			
		case "cast_column":
			if columnName, ok1 := transform.Parameters["column_name"].(string); ok1 {
				if dataType, ok2 := transform.Parameters["data_type"].(string); ok2 {
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.withColumn(\"%s\", col(\"%s\").cast(\"%s\"))\n", 
						columnName, columnName, dataType))
				}
			}
			
		case "fill_na":
			if fillValue, ok := transform.Parameters["fill_value"]; ok {
				if columns, ok2 := transform.Parameters["columns"].([]interface{}); ok2 {
					columnList := make([]string, len(columns))
					for i, col := range columns {
						columnList[i] = fmt.Sprintf("\"%s\"", col.(string))
					}
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.fillna(%v, subset=[%s])\n", 
						fillValue, strings.Join(columnList, ", ")))
				} else {
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.fillna(%v)\n", fillValue))
				}
			}
			
		case "drop_duplicates":
			if columns, ok := transform.Parameters["columns"].([]interface{}); ok {
				columnList := make([]string, len(columns))
				for i, col := range columns {
					columnList[i] = fmt.Sprintf("\"%s\"", col.(string))
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.dropDuplicates([%s])\n", strings.Join(columnList, ", ")))
			} else {
				b.WriteString("df_transformed = df_transformed.dropDuplicates()\n")
			}
			
		case "order_by":
			if columns, ok := transform.Parameters["columns"].([]interface{}); ok {
				var orderList []string
				for _, col := range columns {
					if colMap, ok2 := col.(map[string]interface{}); ok2 {
						columnName := colMap["column"].(string)
						order := colMap["order"].(string)
						if order == "desc" {
							orderList = append(orderList, fmt.Sprintf("desc(\"%s\")", columnName))
						} else {
							orderList = append(orderList, fmt.Sprintf("asc(\"%s\")", columnName))
						}
					}
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.orderBy(%s)\n", strings.Join(orderList, ", ")))
			}
		}
		b.WriteString("\n")
	}
	
	b.WriteString("df_final = df_transformed\n")
	return b.String()
}

func generateTargetCode(target Target) string {
	var b strings.Builder

	switch target.Type {
	case "s3", "s3_compatible":
		// S3 Configuration for target (if different from source)
		b.WriteString("# Target S3 Configuration\n")
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.access.key\", \"%s\")\n", target.AccessKey))
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.secret.key\", \"%s\")\n", target.SecretKey))
		
		if target.Type == "s3_compatible" {
			b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.endpoint\", \"%s\")\n", target.EndpointURL))
			b.WriteString("spark._jsc.hadoopConfiguration().set(\"fs.s3a.path.style.access\", \"true\")\n")
		}
		
		outputPath := fmt.Sprintf("s3a://%s/%s", target.Bucket, target.Path)
		b.WriteString(fmt.Sprintf("\noutput_path = \"%s\"\n", outputPath))
		b.WriteString(fmt.Sprintf("print(f\"\\nMenyimpan DataFrame ke {output_path}...\")\n"))
		
		mode := target.Mode
		if mode == "" {
			mode = "overwrite"
		}
		
		// Write based on format
		switch target.Format {
		case "csv":
			b.WriteString(fmt.Sprintf("df_final.write.csv(output_path, mode=\"%s\", header=True)\n", mode))
		case "json":
			b.WriteString(fmt.Sprintf("df_final.write.json(output_path, mode=\"%s\")\n", mode))
		case "parquet":
			b.WriteString(fmt.Sprintf("df_final.write.parquet(output_path, mode=\"%s\")\n", mode))
		case "orc":
			b.WriteString(fmt.Sprintf("df_final.write.orc(output_path, mode=\"%s\")\n", mode))
		default:
			b.WriteString(fmt.Sprintf("df_final.write.parquet(output_path, mode=\"%s\")\n", mode))
		}
		
		b.WriteString("print(\"DataFrame berhasil disimpan ke S3.\")\n")

	case "postgresql":
		jdbcURL := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", target.Host, target.Port, target.Database)
		b.WriteString("# Target PostgreSQL Configuration\n")
		b.WriteString(fmt.Sprintf("jdbc_url = \"%s\"\n", jdbcURL))
		b.WriteString("properties = {\n")
		b.WriteString(fmt.Sprintf("    \"user\": \"%s\",\n", target.User))
		b.WriteString(fmt.Sprintf("    \"password\": \"%s\",\n", target.Password))
		b.WriteString("    \"driver\": \"org.postgresql.Driver\"\n")
		b.WriteString("}\n")
		
		mode := target.Mode
		if mode == "" {
			mode = "overwrite"
		}
		
		b.WriteString(fmt.Sprintf("df_final.write.jdbc(\n"))
		b.WriteString(fmt.Sprintf("    url=jdbc_url,\n"))
		b.WriteString(fmt.Sprintf("    table=\"%s\",\n", target.Table))
		b.WriteString(fmt.Sprintf("    mode=\"%s\",\n", mode))
		b.WriteString(fmt.Sprintf("    properties=properties\n"))
		b.WriteString(")\n")
		b.WriteString("print(\"DataFrame berhasil disimpan ke PostgreSQL.\")\n")
	}

	return b.String()
}

func handleGenerateScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var job ETLJobRequest
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	script := generatePySparkScript(job)
	
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(script))
}

func handleOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/generate", handleGenerateScript)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			handleOptions(w, r)
			return
		}
		fmt.Fprintf(w, "ETL Script Generator API\nEndpoints:\nPOST /generate - Generate PySpark script")
	})
	
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	
	fmt.Printf("Starting ETL Script Generator server on port %s\n", port)
	fmt.Println("Available endpoints:")
	fmt.Println("  POST /generate - Generate PySpark script")
	
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
