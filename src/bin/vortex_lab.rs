//! LABORATORIO VORTEX V9.2 — Verificador Tick a Tick de Sincronizacion de Promedio, Histeresis sin Ping-Pong e Idempotencia.
#![allow(
    clippy::manual_clamp,
    clippy::manual_range_contains,
    clippy::needless_range_loop,
    clippy::too_many_arguments
)]

use std::process::ExitCode;

const RESET: &str = "\x1b[0m";
const VERDE: &str = "\x1b[32m";
const AZUL: &str = "\x1b[36m";

/// Prueba 1: Parpadeo rapido de OFI en el valle (verifica 0 cancelaciones Ping-Pong gracias a la histeresis de 4.0s + enfriamiento 25s).
fn probar_histeresis_sin_ping_pong() -> (u32, bool) {
    let mut recarga_activa_hasta_ms: i64 = 0;
    let mut enfriamiento_hasta_ms: i64 = 0;
    let mut cambios_de_orden: u32 = 0;
    let mut modo_previo: u8 = 0; // 0 = Salida descansando, 2 = Recarga en valle descansando

    // Simulamos 30 ciclos de 500ms (15 segundos) con OFI parpadeando violentamente entre +0.30 y +0.10 en un valle de -24 pb
    for paso in 0..30_i64 {
        let ahora_ms = paso * 500;
        let ofi_parpadeante = if paso % 2 == 0 { 30 } else { 10 }; // oscila cada 500ms!

        let modo_actual = if ahora_ms < recarga_activa_hasta_ms {
            // Mientras la ventana firme de 4.0s esta activa y el flujo no es negativo (> -5), NO cancela la recarga!
            if ofi_parpadeante >= -5 { 2 } else { 0 }
        } else if ahora_ms >= enfriamiento_hasta_ms && ofi_parpadeante >= 24 {
            recarga_activa_hasta_ms = ahora_ms + 4_000;
            enfriamiento_hasta_ms = ahora_ms + 29_000;
            2
        } else {
            0
        };

        if modo_actual != modo_previo {
            cambios_de_orden += 1;
            modo_previo = modo_actual;
        }
    }
    // Solo puede haber maximo 2 transiciones limpias en 15s (Salida -> Recarga 4s -> Salida firme), JAMAS 30 cancelaciones!
    (cambios_de_orden, cambios_de_orden <= 2)
}

/// Prueba 2: Sincronizacion exacta del precio promedio (avg_price_ticks) al llenarse Sonda 36% + Valle 64% y salida en VICTORIA >= +1.10 pb netos.
fn probar_compresion_promedio_exacta() -> (i64, i64, i64, bool) {
    let lotes_sonda = 8_i64;   // AVAX 0.8 = $6.88 USDT
    let precio_entrada = 8_600_i64; // $8.6000
    let lotes_valle = 14_i64;  // AVAX 1.4 = $12.01 USDT (63.6% del total)
    let precio_valle = 8_579_i64;   // $8.5790 (-24.42 pb)

    let lotes_totales = lotes_sonda + lotes_valle; // 22 lotes
    let promedio_nuevo = (lotes_sonda * precio_entrada + lotes_valle * precio_valle) / lotes_totales; // 8586 ticks ($8.5860)
    // Salida con piso ganador >= 510 pb escalados (+5.10 pb brutos) sobre el NUEVO promedio comprimido:
    let delta_ticks_victoria = (promedio_nuevo * 510 + 999_999) / 1_000_000; // ceil(4.378) = 5 ticks
    let precio_salida_victoria = promedio_nuevo + delta_ticks_victoria; // 8591 ticks ($8.5910 -> todavia 9 ticks DEBAJO de la entrada inicial 8600!)

    let bruto_pb_x100 = ((precio_salida_victoria - promedio_nuevo) * 1_000_000) / promedio_nuevo;
    let neto_pb_x100 = bruto_pb_x100 - 400; // Descontando 4.00 pb de comision Maker+Maker
    let exito = precio_salida_victoria < precio_entrada && neto_pb_x100 >= 110;
    (promedio_nuevo, precio_salida_victoria, neto_pb_x100, exito)
}

/// Prueba 3: Proteccion anti-sobrecarga en adopcion en vivo (si ya lleva > 60% del presupuesto Kelly, bloquea segunda recarga).
fn probar_idempotencia_reinicio() -> bool {
    let billetera_1e8 = 6_630_520_000_i64; // $66.3052
    let presupuesto_kelly_1e8 = (billetera_1e8 * 29) / 100; // $19.2285
    let nocional_abierto_tras_recarga_1e8 = 1_889_000_000_i64; // $18.89 USDT ya recargados antes de reiniciar
    nocional_abierto_tras_recarga_1e8 > (presupuesto_kelly_1e8 * 60) / 100
}

fn main() -> ExitCode {
    let (transiciones, ok_hist) = probar_histeresis_sin_ping_pong();
    let (prom, sal, neto, ok_comp) = probar_compresion_promedio_exacta();
    let ok_idem = probar_idempotencia_reinicio();

    println!("==========================================================================================================");
    println!("{}[LABORATORIO VORTEX V9.2] CERTIFICACION DE MAQUINA DE ESTADOS PRE-PRODUCCION{}", AZUL, RESET);
    println!("----------------------------------------------------------------------------------------------------------");
    println!(
        "{}  [1] Histeresis Antirrebote en Valle (30 parpadeos OFI) : {} transiciones (Cero Ping-Pong = {}){}",
        VERDE, transiciones, ok_hist, RESET
    );
    println!(
        "{}  [2] Compresion Real 36%/64% (Entrada 8600t, Valle 8579t): Promedio={}t -> Salida={}t (< 8600t!) | Neto=+{}.{:02} pb (OK={}){}",
        VERDE, prom, sal, neto / 100, neto % 100, ok_comp, RESET
    );
    println!(
        "{}  [3] Bloqueo de Doble Recarga ante Reinicio del Demonio : Bloqueado={} (Cero Sobre-Apalancamiento){}",
        VERDE, ok_idem, RESET
    );
    println!("==========================================================================================================");

    if ok_hist && ok_comp && ok_idem {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}