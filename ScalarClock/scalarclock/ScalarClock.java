package scalarclock;

class ScalarClock {

private short id;
private int time;

public enum CompareValue {BEFORE, AFTER, CONCURRENT}


private ScalarClock(){}

public ScalarClock(short id){
        time =0;
        this.id=id;
}

private ScalarClock(short id, int time){
        this.time = time;
        this.id=id;
}

public short getId(){
        return id;
}

public int getValue(){
        return time;
}

public ScalarClock increment(){
        return new ScalarClock(id, ++time);
}

public CompareValue compare(ScalarClock obj){
        if (obj.time==time) return CompareValue.CONCURRENT;
        else if (obj.time<time) return CompareValue.AFTER;
        else return CompareValue.BEFORE;
}

@Override
public String toString(){
        return "id: "+id+"time: "+time;
}

}
