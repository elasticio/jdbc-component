package io.elastic.jdbc;

import java.util.Arrays;
import java.util.List;

public class ProcedureParameter {

  private String name;
  private Direction direction;
  private int type;

  public ProcedureParameter(String name, Direction direction, int type) {
    this.name = name;
    this.direction = direction;
    this.type = type;
  }

  public ProcedureParameter(String name, short direction, int type) {
    this.name = name;
    this.direction = jdbcColTypeToDirection(direction);
    this.type = type;
  }

  private Direction jdbcColTypeToDirection(short direction) {
    List<Short> inputTypes = Arrays.asList((short) 1);
    List<Short> outputTypes = Arrays.asList((short) 3, (short) 4);
    List<Short> inOutputTypes = Arrays.asList((short) 2);

    return inputTypes.contains(direction) ? Direction.IN
        : outputTypes.contains(direction) ? Direction.OUT
        : inOutputTypes.contains(direction) ? Direction.INOUT
        : Direction.UNDEFINED;
  }

  public enum Direction {
    IN("in"),
    OUT("out"),
    INOUT("inout"),
    UNDEFINED("undefined");

    private String dirName;

    Direction(String dirName) {
      this.dirName = dirName;
    }

    ;

    @Override
    public String toString() {
      return this.dirName;
    }
  }

  public String getName() {
    return name;
  }

  public Direction getDirection() {
    return direction;
  }

  public int getType() {
    return type;
  }
}
