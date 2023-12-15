package documents

type Config struct {
	Enable  bool    `json:"enable"`
	OpenAPI OpenAPI `json:"oas"`
}

type OpenAPI struct {
	Version     string `json:"version"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Term        string `json:"term"`
}
