package com.datastax.spark.connector.mapper;

import java.io.Serializable;
import java.util.Objects;

public class ClassWithUDTBean implements Serializable
{
    public static class AddressBean implements Serializable
    {
        private String street;
        private String city;
        private Integer zip;

        public AddressBean() {}
        public AddressBean(String street, String city, Integer zip) {
            this.street = street;
            this.city = city;
            this.zip = zip;
        }



        public Integer getZip()
        {
            return zip;
        }

        public String getCity()
        {
            return city;
        }

        public String getStreet()
        {
            return street;
        }

        public void setCity(String city)
        {
            this.city = city;
        }

        public void setStreet(String street)
        {
            this.street = street;
        }

        public void setZip(Integer zip)
        {
            this.zip = zip;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof AddressBean))
                return false;

            AddressBean that = (AddressBean) obj;
            return Objects.equals(that.city, this.city) &&
                    Objects.equals(that.street, this.street) &&
                    Objects.equals(that.zip, this.zip);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(city, street, zip);
        }
    }

    private Integer key;
    private String name;
    private AddressBean addr;

    public ClassWithUDTBean() {}
    public ClassWithUDTBean(Integer key, String name, AddressBean addr) {
        this.key = key;
        this.name = name;
        this.addr = addr;
    }

    public AddressBean getAddr()
    {
        return addr;
    }

    public Integer getKey()
    {
        return key;
    }

    public String getName()
    {
        return name;
    }

    public void setAddr(AddressBean addr)
    {
        this.addr = addr;
    }

    public void setKey(Integer key)
    {
        this.key = key;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Override
    public String toString()
    {
        return key + " : " + name + " : " + addr;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof ClassWithUDTBean))
            return false;

        ClassWithUDTBean that = (ClassWithUDTBean) obj;
        return Objects.equals(this.addr, that.addr) &&
                Objects.equals(this.key, that.key) &&
                Objects.equals(this.name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(addr, key, name);
    }
}
