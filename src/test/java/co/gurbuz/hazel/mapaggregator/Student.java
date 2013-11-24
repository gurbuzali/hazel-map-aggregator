package co.gurbuz.hazel.mapaggregator;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * @ali 23/11/13
 */
public class Student implements DataSerializable, Comparable<Student> {

    String name;

    String className;

    String schoolName;

    int note;

    public Student() {

    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getSchoolName() {
        return schoolName;
    }

    public int getNote() {
        return note;
    }

    public Student(String name, String className, String schoolName, int note) {
        this.name = name;
        this.className = className;
        this.schoolName = schoolName;
        this.note = note;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(className);
        out.writeUTF(schoolName);
        out.writeInt(note);
    }

    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        className = in.readUTF();
        schoolName = in.readUTF();
        note = in.readInt();
    }

    public int compareTo(Student o) {
        return note - o.note;
    }
}
