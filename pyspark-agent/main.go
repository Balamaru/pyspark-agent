package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type ETLJobRequest struct {
	JobName         string      `json:"jobName"`
	Source          DataSource  `json:"source"`
	Transforms      []Transform `json:"transforms,omitempty"`
	Target          *Target     `json:"target,omitempty"`
	CreateConfigMap bool        `json:"createConfigMap,omitempty"`
	ConfigMapName   string      `json:"configMapName,omitempty"`
	Namespace       string      `json:"namespace,omitempty"`
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

type GenerateResponse struct {
	Script          string `json:"script"`
	ConfigMapName   string `json:"configMapName,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
	ConfigMapStatus string `json:"configMapStatus,omitempty"`
}

var kubeClient *kubernetes.Clientset

// Kubernetes Client Initialization
func initKubernetesClient() error {
	var config *rest.Config
	var err error

	config, err = rest.InClusterConfig()
	if err != nil {
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return fmt.Errorf("failed to create kubernetes config: %v", err)
		}
	}

	kubeClient, err = kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes client: %v", err)
	}

	log.Println("Kubernetes client initialized successfully")
	return nil
}

// Create/Update ConfigMap (With Annotation)
func createConfigMapWithAnnotations(namespace, name, fileName, scriptContent, jobJson string) error {
	if kubeClient == nil {
		return fmt.Errorf("kubernetes client not initialized")
	}

	configMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app":         "etl-generator",
				"generated":   "true",
				"script-type": "pyspark",
			},
			Annotations: map[string]string{
				"etl-job-definition": jobJson,
			},
		},
		Data: map[string]string{
			fileName: scriptContent,
		},
	}

	ctx := context.Background()
	existingCM, err := kubeClient.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})

	if err == nil {
		existingCM.Data = configMap.Data
		existingCM.Labels = configMap.Labels
		existingCM.Annotations = configMap.Annotations
		_, err = kubeClient.CoreV1().ConfigMaps(namespace).Update(ctx, existingCM, metav1.UpdateOptions{})
		return err
	}

	_, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(ctx, configMap, metav1.CreateOptions{})
	return err
}

// Script Generator
func generatePySparkScript(job ETLJobRequest) string {
	var b strings.Builder

	b.WriteString("from pyspark.sql import SparkSession\n")
	b.WriteString("from pyspark.sql.functions import *\n")
	b.WriteString("from pyspark.sql.types import *\n\n")

	b.WriteString(fmt.Sprintf("spark = SparkSession.builder.appName(\"%s\").getOrCreate()\n\n", job.JobName))
	b.WriteString(generateSourceCode(job.Source))
	b.WriteString("\n")

	if len(job.Transforms) > 0 {
		b.WriteString("# Apply transformations\n")
		b.WriteString(generateTransformCode(job.Transforms))
		b.WriteString("\n")
	} else {
		b.WriteString("df_final = df\n\n")
	}

	if job.Target != nil {
		b.WriteString(generateTargetCode(*job.Target))
	}

	b.WriteString("\nspark.stop()\n")
	return b.String()
}

// Source Code Generator
func generateSourceCode(source DataSource) string {
	var b strings.Builder

	switch source.Type {

	case "s3", "s3_compatible":
		b.WriteString("# S3 Configuration\n")
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.access.key\", \"%s\")\n", source.AccessKey))
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.secret.key\", \"%s\")\n", source.SecretKey))

		if source.Type == "s3_compatible" {
			b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.endpoint\", \"%s\")\n", source.EndpointURL))
			b.WriteString("spark._jsc.hadoopConfiguration().set(\"fs.s3a.path.style.access\", \"true\")\n")
		}

		path := fmt.Sprintf("s3a://%s/%s", source.Bucket, source.Path)
		b.WriteString(fmt.Sprintf("\npath = \"%s\"\n", path))

		switch source.Format {
		case "csv":
			b.WriteString("df = spark.read.csv(path, header=True, inferSchema=True)\n")
		case "json":
			b.WriteString("df = spark.read.json(path, multiLine=True)\n")
		case "parquet":
			b.WriteString("df = spark.read.parquet(path)\n")
		default:
			b.WriteString("df = spark.read.csv(path, header=True, inferSchema=True)\n")
		}

	case "postgresql":
		jdbcURL := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", source.Host, source.Port, source.Database)
		b.WriteString("# PostgreSQL Configuration\n")
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

// Transform Code
func generateTransformCode(transforms []Transform) string {
	var b strings.Builder

	b.WriteString("df_transformed = df\n\n")

	for i, transform := range transforms {
		b.WriteString(fmt.Sprintf("# Transform %d: %s\n", i+1, transform.Type))

		switch transform.Type {

		case "filter":
			if cond, ok := transform.Parameters["condition"].(string); ok {
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.filter(\"%s\")\n", cond))
			}

		case "select":
			if cols, ok := transform.Parameters["columns"].([]interface{}); ok {
				var list []string
				for _, col := range cols {
					list = append(list, fmt.Sprintf("\"%s\"", col.(string)))
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.select(%s)\n", strings.Join(list, ", ")))
			}

		case "rename_column":
			oldName, _ := transform.Parameters["old_name"].(string)
			newName, _ := transform.Parameters["new_name"].(string)
			b.WriteString(fmt.Sprintf("df_transformed = df_transformed.withColumnRenamed(\"%s\", \"%s\")\n", oldName, newName))

		case "drop_column":
			if cols, ok := transform.Parameters["columns"].([]interface{}); ok {
				for _, col := range cols {
					b.WriteString(fmt.Sprintf("df_transformed = df_transformed.drop(\"%s\")\n", col.(string)))
				}
			}

		case "add_column":
			col, _ := transform.Parameters["column_name"].(string)
			expr, _ := transform.Parameters["expression"].(string)
			b.WriteString(fmt.Sprintf("df_transformed = df_transformed.withColumn(\"%s\", %s)\n", col, expr))

		case "join":
			right, _ := transform.Parameters["right_table"].(string)
			cond, _ := transform.Parameters["join_condition"].(string)
			jtype, _ := transform.Parameters["join_type"].(string)
			b.WriteString(fmt.Sprintf("df_transformed = df_transformed.join(%s, %s, \"%s\")\n", right, cond, jtype))

		case "groupby":
			groupCols, _ := transform.Parameters["group_by"].([]interface{})
			aggs, _ := transform.Parameters["aggregations"].(map[string]interface{})

			var gcols []string
			for _, gc := range groupCols {
				gcols = append(gcols, fmt.Sprintf("\"%s\"", gc.(string)))
			}

			var aggList []string
			for col, fun := range aggs {
				aggList = append(aggList, fmt.Sprintf("%s(\"%s\")", fun.(string), col))
			}

			b.WriteString(fmt.Sprintf("df_transformed = df_transformed.groupBy(%s).agg(%s)\n",
				strings.Join(gcols, ", "), strings.Join(aggList, ", ")))

		case "sql":
			query, _ := transform.Parameters["query"].(string)
			temp, _ := transform.Parameters["temp_view"].(string)
			b.WriteString(fmt.Sprintf("df_transformed.createOrReplaceTempView(\"%s\")\n", temp))
			b.WriteString(fmt.Sprintf("df_transformed = spark.sql(\"\"\"%s\"\"\")\n", query))

		case "cast_column":
			col, _ := transform.Parameters["column_name"].(string)
			typ, _ := transform.Parameters["data_type"].(string)
			b.WriteString(fmt.Sprintf("df_transformed = df_transformed.withColumn(\"%s\", col(\"%s\").cast(\"%s\"))\n",
				col, col, typ))

		case "fill_na":
			val := transform.Parameters["fill_value"]
			var valString string

			switch v := val.(type) {
			case string:
				valString = fmt.Sprintf("\"%s\"", v)
			case float64, int:
				valString = fmt.Sprintf("%v", v)
			default:
				valString = fmt.Sprintf("\"%v\"", v)
			}

			if cols, ok := transform.Parameters["columns"].([]interface{}); ok {
				var clist []string
				for _, c := range cols {
					clist = append(clist, fmt.Sprintf("\"%s\"", c.(string)))
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.fillna(%s, subset=[%s])\n",
					valString, strings.Join(clist, ", ")))
			} else {
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.fillna(%s)\n", valString))
			}

		case "drop_duplicates":
			if cols, ok := transform.Parameters["columns"].([]interface{}); ok {
				var list []string
				for _, c := range cols {
					list = append(list, fmt.Sprintf("\"%s\"", c.(string)))
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.dropDuplicates([%s])\n",
					strings.Join(list, ", ")))
			} else {
				b.WriteString("df_transformed = df_transformed.dropDuplicates()\n")
			}

		case "order_by":
			if cols, ok := transform.Parameters["columns"].([]interface{}); ok {
				var list []string
				for _, c := range cols {
					row := c.(map[string]interface{})
					name := row["column"].(string)
					order := row["order"].(string)
					if order == "desc" {
						list = append(list, fmt.Sprintf("desc(\"%s\")", name))
					} else {
						list = append(list, fmt.Sprintf("asc(\"%s\")", name))
					}
				}
				b.WriteString(fmt.Sprintf("df_transformed = df_transformed.orderBy(%s)\n", strings.Join(list, ", ")))
			}
		}

		b.WriteString("\n")
	}

	b.WriteString("df_final = df_transformed\n")
	return b.String()
}

// Target Code Generator
func generateTargetCode(target Target) string {
	var b strings.Builder

	switch target.Type {

	case "s3", "s3_compatible":
		b.WriteString("# Target S3 Configuration\n")
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.access.key\", \"%s\")\n", target.AccessKey))
		b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.secret.key\", \"%s\")\n", target.SecretKey))

		if target.Type == "s3_compatible" {
			b.WriteString(fmt.Sprintf("spark._jsc.hadoopConfiguration().set(\"fs.s3a.endpoint\", \"%s\")\n", target.EndpointURL))
			b.WriteString("spark._jsc.hadoopConfiguration().set(\"fs.s3a.path.style.access\", \"true\")\n")
		}

		out := fmt.Sprintf("s3a://%s/%s", target.Bucket, target.Path)
		b.WriteString(fmt.Sprintf("\noutput_path = \"%s\"\n", out))

		mode := target.Mode
		if mode == "" {
			mode = "overwrite"
		}

		switch target.Format {
		case "csv":
			b.WriteString(fmt.Sprintf("df_final.write.csv(output_path, mode=\"%s\", header=True)\n", mode))
		case "json":
			b.WriteString(fmt.Sprintf("df_final.write.json(output_path, mode=\"%s\")\n", mode))
		case "parquet":
			b.WriteString(fmt.Sprintf("df_final.write.parquet(output_path, mode=\"%s\")\n", mode))
		default:
			b.WriteString(fmt.Sprintf("df_final.write.parquet(output_path, mode=\"%s\")\n", mode))
		}

	case "postgresql":
		jdbc := fmt.Sprintf("jdbc:postgresql://%s:%d/%s", target.Host, target.Port, target.Database)
		b.WriteString(fmt.Sprintf("jdbc_url = \"%s\"\n", jdbc))
		b.WriteString("properties = {\n")
		b.WriteString(fmt.Sprintf("    \"user\": \"%s\",\n", target.User))
		b.WriteString(fmt.Sprintf("    \"password\": \"%s\",\n", target.Password))
		b.WriteString("    \"driver\": \"org.postgresql.Driver\"\n")
		b.WriteString("}\n")

		mode := target.Mode
		if mode == "" {
			mode = "overwrite"
		}

		b.WriteString("df_final.write.jdbc(\n")
		b.WriteString(fmt.Sprintf("    url=jdbc_url,\n"))
		b.WriteString(fmt.Sprintf("    table=\"%s\",\n", target.Table))
		b.WriteString(fmt.Sprintf("    mode=\"%s\",\n", mode))
		b.WriteString("    properties=properties\n")
		b.WriteString(")\n")
	}

	return b.String()
}

// JSON Merge Helper
func mergeMaps(orig, patch map[string]interface{}) map[string]interface{} {
	for k, v := range patch {
		orig[k] = v
	}
	return orig
}

// POST /generate
func handleGenerateScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", 405)
		return
	}

	var job ETLJobRequest
	if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
		http.Error(w, "Invalid request: "+err.Error(), 400)
		return
	}

	script := generatePySparkScript(job)

	resp := GenerateResponse{
		Script: script,
	}

	if job.CreateConfigMap {
		namespace := job.Namespace
		if namespace == "" {
			namespace = "spark"
		}

		cmName := job.ConfigMapName
		if cmName == "" {
			cmName = strings.ToLower(strings.ReplaceAll(job.JobName, " ", "-"))
		}

		fileName := fmt.Sprintf("%s.py", strings.ToLower(strings.ReplaceAll(job.JobName, " ", "_")))
		jobJson, _ := json.Marshal(job)

		err := createConfigMapWithAnnotations(namespace, cmName, fileName, script, string(jobJson))
		if err != nil {
			resp.ConfigMapStatus = fmt.Sprintf("Failed: %v", err)
		} else {
			resp.ConfigMapStatus = "Created/Updated successfully"
		}

		resp.ConfigMapName = cmName
		resp.Namespace = namespace
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GET /jobs — List All ETL Jobs
func handleGetJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", 405)
		return
	}

	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = "spark"
	}

	ctx := context.Background()

	cms, err := kubeClient.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=etl-generator,generated=true,script-type=pyspark",
	})
	if err != nil {
		http.Error(w, "Cannot list jobs: "+err.Error(), 500)
		return
	}

	var result []map[string]interface{}

	for _, cm := range cms.Items {
		entry := map[string]interface{}{
			"name":        cm.Name,
			"namespace":   cm.Namespace,
			"labels":      cm.Labels,
			"annotations": cm.Annotations,
		}
		result = append(result, entry)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// UNIVERSAL HANDLER
// GET /job/{name}
// PATCH /job/{name}
func handleJobByName(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 2 || parts[0] != "job" {
		http.Error(w, "Invalid path. Use /job/{name}", 400)
		return
	}

	name := parts[1]
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = "spark"
	}

	switch r.Method {

	// GET /job/{name}
	case http.MethodGet:
		ctx := context.Background()

		cm, err := kubeClient.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			http.Error(w, "ConfigMap not found: "+err.Error(), 404)
			return
		}

		var script string
		for _, v := range cm.Data {
			script = v
			break
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"name":        cm.Name,
			"namespace":   cm.Namespace,
			"labels":      cm.Labels,
			"annotations": cm.Annotations,
			"script":      script,
		})
		return

	// PATCH /job/{name}
	case http.MethodPatch:
		ctx := context.Background()

		cm, err := kubeClient.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			http.Error(w, "ConfigMap not found: "+err.Error(), 404)
			return
		}

		existing := cm.Annotations["etl-job-definition"]
		if existing == "" {
			http.Error(w, "No etl-job-definition found", 400)
			return
		}

		var original map[string]interface{}
		json.Unmarshal([]byte(existing), &original)

		var patch map[string]interface{}
		json.NewDecoder(r.Body).Decode(&patch)

		merged := mergeMaps(original, patch)
		mergedBytes, _ := json.Marshal(merged)

		var job ETLJobRequest
		json.Unmarshal(mergedBytes, &job)

		newScript := generatePySparkScript(job)
		fileName := fmt.Sprintf("%s.py", strings.ToLower(strings.ReplaceAll(job.JobName, " ", "_")))

		patchObj := map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": map[string]interface{}{
					"etl-job-definition": string(mergedBytes),
				},
			},
			"data": map[string]interface{}{
				fileName: newScript,
			},
		}

		patchJson, _ := json.Marshal(patchObj)

		_, err = kubeClient.CoreV1().ConfigMaps(ns).Patch(
			ctx,
			name,
			types.StrategicMergePatchType,
			patchJson,
			metav1.PatchOptions{},
		)

		if err != nil {
			http.Error(w, "Patch failed: "+err.Error(), 500)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message":       "Job updated successfully",
			"mergedJob":     job,
			"updatedScript": newScript,
		})
		return

	default:
		http.Error(w, "Method not allowed", 405)
		return
	}
}

// Main
func main() {
	if err := initKubernetesClient(); err != nil {
		log.Printf("Warning: Kubernetes client initialization failed: %v", err)
	}

	// Health check endpoint (penting!)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/generate", handleGenerateScript)
	http.HandleFunc("/jobs", handleGetJobs)
	http.HandleFunc("/job/", handleJobByName)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("ETL Script Generator Server started on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}