package pipelite.entity;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

@Converter(autoApply = true)
public class BooleanConverter implements AttributeConverter<Boolean, Character> {

  @Override
  public Character convertToDatabaseColumn(Boolean attribute) {
    if (attribute != null) {
      if (attribute) {
        return 'Y';
      } else {
        return 'N';
      }
    }
    return null;
  }

  @Override
  public Boolean convertToEntityAttribute(Character dbData) {
    if (dbData != null) {
      return dbData.equals('Y');
    }
    return null;
  }
}
