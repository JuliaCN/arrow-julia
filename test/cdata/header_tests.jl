@testset "exposes a C consumer header" begin
    @test isfile(CData.header_path())
    @test basename(CData.header_path()) == "arrow_julia_cdata.h"
end
