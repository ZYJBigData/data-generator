package com.bizseer.bigdata.clickhouse;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

/**
 * @author xiebo
 * @date 2022/6/17 1:38 下午
 */

@Getter
public class ColDescriptor {

    public final static String TYPE_DESC_INTEGER = "integer";

    public final static String TYPE_DESC_NUMBER = "number";

    public final static String TYPE_DESC_STRING = "string";

    public final static String TYPE_DESC_BOOLEAN = "boolean";

    public final static String TYPE_DESC_OBJECT = "object";

    /**
     * 匹配类型符点型长度 (4,2) 或者 4,2 这种格式
     */
    private static Pattern MD_LENGTH_PATTERN = Pattern.compile("^[\\(]?+[ ]*+[0-9]+[ ]*+,+[ ]*+[0-9]+[ ]*+[\\)]?+$");

    public static InputLengthValidator IGNORE_VALIDATOR = new InputLengthValidator() {
        @Override
        public boolean validate(String inputLength) {
            return true;
        }
    };

    public static InputLengthValidator STRING_LENGTH_VALIDATOR = new InputLengthValidator() {
        @Override
        public boolean validate(String inputLength) {
            // 不能为空且为整数
            return StringUtils.isNotBlank(inputLength) && StringUtils.isNumeric(inputLength);
        }
    };

    public static InputLengthValidator INT_LENGTH_VALIDATOR = new InputLengthValidator() {
        @Override
        public boolean validate(String inputLength) {
            // 不能为空且为整数
            return StringUtils.isNotBlank(inputLength) && StringUtils.isNumeric(inputLength);
        }
    };

    public static InputLengthValidator FLOAT_LENGTH_VALIDATOR = new InputLengthValidator() {
        @Override
        public boolean validate(String inputLength) {
            // 浮点形长度可以不设置
            if(StringUtils.isBlank(inputLength)){
                return true;
            }
            // 为 M,D 或者 (M,D) 类型
            return MD_LENGTH_PATTERN.matcher(inputLength).matches();

//            // 没有长度，或 d,f 格式
//            if(StringUtils.isBlank(inputLength)){
//                return true;
//            }
//            String[] arr = inputLength.split(",");
//            if(arr.length == 1){
//                return StringUtils.isNumeric(inputLength);
//            }
//            if(arr.length == 2){
//                return StringUtils.isNumeric(arr[0]) && StringUtils.isNumeric(arr[1]);
//            }
//            return false;
        }
    };

    public static InputLengthValidator TIME_LENGTH_VALIDATOR = new InputLengthValidator() {
        @Override
        public boolean validate(String inputLength) {
            // 时间类型不能有长度
            return StringUtils.isBlank(inputLength);
        }
    };

  private String typeName;

  private InputLengthValidator inputLengthValidator;

  private boolean timedField = false;

  private boolean needLength = true;

  private String typeDesc = "";

  private String lengthValidateErrMsg;

    public ColDescriptor(String typeName, InputLengthValidator inputLengthValidator, boolean timedField, boolean needLength, String typeDesc) {
        this.typeName = typeName;
        this.inputLengthValidator = inputLengthValidator;
        this.timedField = timedField;
        this.needLength = needLength;
        this.typeDesc = typeDesc;
    }

    public ColDescriptor setLengthValidateErrMsg(String lengthValidateErrMsg) {
        this.lengthValidateErrMsg = lengthValidateErrMsg;
        return this;
    }

    public static interface InputLengthValidator{
      public boolean validate(String inputLength);
  }

}
