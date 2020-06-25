package com.epam.vladkabat;

import java.io.Serializable;

public class WeatherHotel implements Serializable {

  private long hotel_id;
  private String hotel_name;
  private String weather_wthr_date;
  private double weather_avg_tmpr_c;
  private int weather_year;
  private int weather_month;
  private int weather_day;

  @Override
  public String toString() {
    return "WeatherHotel{" +
        "hotel_id=" + hotel_id +
        ", hotel_name='" + hotel_name + '\'' +
        ", weather_wthr_date='" + weather_wthr_date + '\'' +
        ", weather_avg_tmpr_c=" + weather_avg_tmpr_c +
        ", weather_year=" + weather_year +
        ", weather_month=" + weather_month +
        ", weather_day=" + weather_day +
        '}';
  }

  public long getHotel_id() {
    return hotel_id;
  }

  public void setHotel_id(long hotel_id) {
    this.hotel_id = hotel_id;
  }

  public String getHotel_name() {
    return hotel_name;
  }

  public void setHotel_name(String hotel_name) {
    this.hotel_name = hotel_name;
  }

  public String getWeather_wthr_date() {
    return weather_wthr_date;
  }

  public void setWeather_wthr_date(String weather_wthr_date) {
    this.weather_wthr_date = weather_wthr_date;
  }

  public double getWeather_avg_tmpr_c() {
    return weather_avg_tmpr_c;
  }

  public void setWeather_avg_tmpr_c(double weather_avg_tmpr_c) {
    this.weather_avg_tmpr_c = weather_avg_tmpr_c;
  }

  public int getWeather_year() {
    return weather_year;
  }

  public void setWeather_year(int weather_year) {
    this.weather_year = weather_year;
  }

  public int getWeather_month() {
    return weather_month;
  }

  public void setWeather_month(int weather_month) {
    this.weather_month = weather_month;
  }

  public int getWeather_day() {
    return weather_day;
  }

  public void setWeather_day(int weather_day) {
    this.weather_day = weather_day;
  }
}
