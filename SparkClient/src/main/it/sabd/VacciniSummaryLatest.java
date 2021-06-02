package it.sabd;
import java.io.Serializable;
import java.util.Date;


public class VacciniSummaryLatest implements Serializable {

    private Date data_somministrazione;
    private String area;
    private int totale;
    private int sesso_maschile;
    private int sesso_femminile;
    private int categoria_operatori_sanitari_sociosanitari;
    private int categoria_personale_non_sanitario;
    private int categoria_ospiti_rsa;
    private int categoria_personale_scolastico;
    private int categoria_60_69;
    private int categoria_70_79;
    private int categoria_over80;
    private int categoria_soggetti_fragili;
    private int categoria_forze_armate;
    private int categoria_altro;
    private int prima_dose;
    private int seconda_dose;
    private String codice_NUTS1;
    private String codice_NUTS2;
    private int codice_regione_ISTAT;
    private String nome_area;




    @Override
    public String toString(){
        return getData_somministrazione() + ", " + getArea() + ", " + getTotale() + ", " + getSesso_maschile() + ", "
                + getSesso_femminile() + ", " + getCategoria_operatori_sanitari_sociosanitari() + ", " +
                getCategoria_personale_non_sanitario() + ", " + getCategoria_ospiti_rsa() + ", " +
                getCategoria_personale_scolastico() + ", " + getCategoria_60_69() + ", " + getCategoria_70_79() + ", " +
                getCategoria_over80() + ", " + getCategoria_soggetti_fragili() + ", " + getCategoria_forze_armate() + ", " +
                getCategoria_altro() + ", " + getPrima_dose() + ", " + getSeconda_dose() + ", " + getCodice_NUTS1() + ", " +
                getCodice_NUTS2() + ", " + getCodice_regione_ISTAT() + ", " + getNome_area();
    }

    public int getTotale() {
        return totale;
    }

    public void setTotale(int totale) {
        this.totale = totale;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public Date getData_somministrazione() {
        return data_somministrazione;
    }

    public void setData_somministrazione(Date data_somministrazione) {
        this.data_somministrazione = data_somministrazione;
    }

    public int getSesso_maschile() {
        return sesso_maschile;
    }

    public void setSesso_maschile(int sesso_maschile) {
        this.sesso_maschile = sesso_maschile;
    }

    public int getSesso_femminile() {
        return sesso_femminile;
    }

    public void setSesso_femminile(int sesso_femminile) {
        this.sesso_femminile = sesso_femminile;
    }

    public int getCategoria_operatori_sanitari_sociosanitari() {
        return categoria_operatori_sanitari_sociosanitari;
    }

    public void setCategoria_operatori_sanitari_sociosanitari(int categoria_operatori_sanitari_sociosanitari) {
        this.categoria_operatori_sanitari_sociosanitari = categoria_operatori_sanitari_sociosanitari;
    }

    public int getCategoria_personale_non_sanitario() {
        return categoria_personale_non_sanitario;
    }

    public void setCategoria_personale_non_sanitario(int categoria_personale_non_sanitario) {
        this.categoria_personale_non_sanitario = categoria_personale_non_sanitario;
    }

    public int getCategoria_ospiti_rsa() {
        return categoria_ospiti_rsa;
    }

    public void setCategoria_ospiti_rsa(int categoria_ospiti_rsa) {
        this.categoria_ospiti_rsa = categoria_ospiti_rsa;
    }

    public int getCategoria_personale_scolastico() {
        return categoria_personale_scolastico;
    }

    public void setCategoria_personale_scolastico(int categoria_personale_scolastico) {
        this.categoria_personale_scolastico = categoria_personale_scolastico;
    }

    public int getCategoria_60_69() {
        return categoria_60_69;
    }

    public void setCategoria_60_69(int categoria_60_69) {
        this.categoria_60_69 = categoria_60_69;
    }

    public int getCategoria_70_79() {
        return categoria_70_79;
    }

    public void setCategoria_70_79(int categoria_70_79) {
        this.categoria_70_79 = categoria_70_79;
    }

    public int getCategoria_over80() {
        return categoria_over80;
    }

    public void setCategoria_over80(int categoria_over80) {
        this.categoria_over80 = categoria_over80;
    }

    public int getCategoria_soggetti_fragili() {
        return categoria_soggetti_fragili;
    }

    public void setCategoria_soggetti_fragili(int categoria_soggetti_fragili) {
        this.categoria_soggetti_fragili = categoria_soggetti_fragili;
    }

    public int getCategoria_forze_armate() {
        return categoria_forze_armate;
    }

    public void setCategoria_forze_armate(int categoria_forze_armate) {
        this.categoria_forze_armate = categoria_forze_armate;
    }

    public int getCategoria_altro() {
        return categoria_altro;
    }

    public void setCategoria_altro(int categoria_altro) {
        this.categoria_altro = categoria_altro;
    }

    public int getPrima_dose() {
        return prima_dose;
    }

    public void setPrima_dose(int prima_dose) {
        this.prima_dose = prima_dose;
    }

    public int getSeconda_dose() {
        return seconda_dose;
    }

    public void setSeconda_dose(int seconda_dose) {
        this.seconda_dose = seconda_dose;
    }

    public String getCodice_NUTS1() {
        return codice_NUTS1;
    }

    public void setCodice_NUTS1(String codice_NUTS1) {
        this.codice_NUTS1 = codice_NUTS1;
    }

    public String getCodice_NUTS2() {
        return codice_NUTS2;
    }

    public void setCodice_NUTS2(String codice_NUTS2) {
        this.codice_NUTS2 = codice_NUTS2;
    }

    public int getCodice_regione_ISTAT() {
        return codice_regione_ISTAT;
    }

    public void setCodice_regione_ISTAT(int codice_regione_ISTAT) {
        this.codice_regione_ISTAT = codice_regione_ISTAT;
    }

    public String getNome_area() {
        return nome_area;
    }

    public void setNome_area(String nome_area) {
        this.nome_area = nome_area;
    }
}
