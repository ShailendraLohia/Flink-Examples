package Udemy_Course.Assignment;

public class RideData {
    private String cab_id;
    private String cab_number_plate;
    private String cab_type;
    private String driver_name;
    private String ongoing_trip;
    private String pickup_location;
    private String destination;
    private int numberOfPassenger;

    public String getCab_id() {
        return cab_id;
    }

    public void setCab_id(String cab_id) {
        this.cab_id = cab_id;
    }

    public String getCab_number_plate() {
        return cab_number_plate;
    }

    public void setCab_number_plate(String cab_number_plate) {
        this.cab_number_plate = cab_number_plate;
    }

    public String getCab_type() {
        return cab_type;
    }

    public void setCab_type(String cab_type) {
        this.cab_type = cab_type;
    }

    public String getDriver_name() {
        return driver_name;
    }

    public void setDriver_name(String driver_name) {
        this.driver_name = driver_name;
    }

    public String getOngoing_trip() {
        return ongoing_trip;
    }

    public void setOngoing_trip(String ongoing_trip) {
        this.ongoing_trip = ongoing_trip;
    }

    public String getPickup_location() {
        return pickup_location;
    }

    public void setPickup_location(String pickup_location) {
        this.pickup_location = pickup_location;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getNumberOfPassenger() {
        return numberOfPassenger;
    }

    public void setNumberOfPassenger(int numberOfPassenger) {
        this.numberOfPassenger = numberOfPassenger;
    }


}
