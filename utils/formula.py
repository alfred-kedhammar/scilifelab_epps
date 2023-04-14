
# To keep be explicit, define four functions from the same formula
def ng_to_fmol(ng, bp):
    """
    Converts ng --> fmol (or ng/ul --> nM)
    Formula based on NEBioCalculator
    https://nebiocalculator.neb.com/#!/dsdnaamt
    """
    return 10**6 * ng / (bp * 617.96 + 36.04)

def ng_ul_to_nM(ng_ul, bp):
    return ng_to_fmol(ng_ul, bp)


def fmol_to_ng(fmol, bp):
    """
    Converts fmol --> ng (or nM to ng/ul)
    Formula based on NEBioCalculator
    https://nebiocalculator.neb.com/#!/dsdnaamt 
    """
    return fmol * (bp * 617.96 + 36.04) / 10**6

def nM_to_ng_ul(nM, bp):
    return fmol_to_ng(nM, bp)