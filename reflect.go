package mgox

import "reflect"

func getElem(t reflect.Type, value reflect.Value) (reflect.Type, reflect.Value) {
	switch t.Kind() {
	case reflect.Ptr:
		//		fmt.Printf("\nDetected Ptr")
		return getElem(t.Elem(), value.Elem())
	case reflect.Slice:
		//		fmt.Printf("\nDetected Slice %d", value.Len())
		var val reflect.Value
		if value.Len() > 0 {
			val = value.Index(0)
		}
		return getElem(t.Elem(), val)
	case reflect.Array:
		//		fmt.Printf("\nDetected Array %d", value.Len())
		var val reflect.Value
		if value.Len() > 0 {
			val = value.Index(0)
		}
		return t.Elem(), val
	case reflect.Interface: // ????
		//		fmt.Printf("\nDetected Interface")
		return reflect.TypeOf(value), reflect.ValueOf(value)
	}
	return t, value
}

func IsSlice(v interface{}) bool {
	resultv := reflect.ValueOf(v)
	return resultv.Kind() == reflect.Ptr && resultv.Elem().Kind() == reflect.Slice
}

func GetValueLen(v interface{}) int {
	if IsSlice(v) {
		return reflect.ValueOf(v).Elem().Len()
	} else {
		return 1
	}
}