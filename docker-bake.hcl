variable "REGISTRY" {
  default = ""
}

group "default" {
  targets = [
    "operator",
    "strim",
    "peggy"
  ]
}

target "base" {
  context    = "./"
  dockerfile = "Dockerfile.base"
  tags       = ["${REGISTRY}thavlik/strim-base:latest"]
  push       = false
}

target "strim" {
  contexts   = { base_context = "target:base" }
  context    = "./"
  dockerfile = "strim/Dockerfile"
  args       = { BASE_IMAGE = "base_context" }
  tags       = ["${REGISTRY}thavlik/strim:latest"]
  push       = true
}

target "peggy" {
  contexts   = { base_context = "target:base" }
  context    = "./"
  dockerfile = "peggy/Dockerfile"
  args       = { BASE_IMAGE = "base_context" }
  tags       = ["${REGISTRY}thavlik/strim-peggy:latest"]
  push       = true
}

target "operator" {
  contexts   = { base_context = "target:base" }
  context    = "./"
  dockerfile = "operator/Dockerfile"
  args       = { BASE_IMAGE = "base_context" }
  tags       = ["${REGISTRY}thavlik/strim-operator:latest"]
  push       = true
}
