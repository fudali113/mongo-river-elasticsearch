package river

import "fmt"

func cleanupInterfaceArray(in []interface{}) []interface{} {
	res := make([]interface{}, len(in))
	for i, v := range in {
		res[i] = cleanupMapValue(v)
	}
	return res
}

func cleanupInterfaceMap(in map[interface{}]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for k, v := range in {
		res[fmt.Sprintf("%v", k)] = cleanupMapValue(v)
	}
	return res
}

func cleanupStringInterfaceMap(in map[string]interface{}) map[string]interface{} {
	for k, v := range in {
		in[k] = cleanupMapValue(v)
	}
	return in
}

func cleanupMapValue(v interface{}) interface{} {
	switch v := v.(type) {
	case []interface{}:
		return cleanupInterfaceArray(v)
	case map[interface{}]interface{}:
		return cleanupInterfaceMap(v)
	case map[string]interface{}:
		return cleanupStringInterfaceMap(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}