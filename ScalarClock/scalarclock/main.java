package scalarclock;

class TestScalar{
  public static void main(String[] args){
    ScalarClock c1 = new ScalarClock((short)1);
    ScalarClock c2 = new ScalarClock((short)2);

    c1 = c1.increment();

    System.out.println(c1);
    System.out.println(c2);

    System.out.println(c1.compare(c2));
    System.out.println(c2.compare(c1));
  }
}
